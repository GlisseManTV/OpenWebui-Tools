"""
title: Message Loading Indicator
description: Adds Message Loading Indicator and Thinking Indicator settings under Settings > Interface, so each user picks how a reply shows it is loading (Open WebUI's default, blinking cursor, pulsing dot, legacy bars, terminal block, typing dots, equalizer, spinner, shimmer lines, ellipsis, scanner, orbit, ripple, breathing orb, dot grid, flipping square, sparkle, heartbeat or bouncing ball) and which icon sits beside the Thinking header while a model reasons. Published into /static/loader.js through the Shared Assets Protocol, so it coexists with Theme Designer Pro and any other plugin using it.
author: @G30
author_url: https://openwebui.com/u/g30
funding_url: https://buymeacoffee.com/iamg30
version: 1.2.0
license: MIT
required_open_webui_version: 0.11.0
"""

import hashlib
import json as _json

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field


import asyncio
import logging
import uuid
from typing import Any, Optional

from open_webui.utils.json_codec import JSONCodec

ASSET_KEY = "message-loading-indicator"  # fixed, used for asset_register too

# ===========================================================================
# Live reload broadcast
# ---------------------------------------------------------------------------
# Each load publishes over Redis; a peer whose cached code differs from the
# database re-executes it. register(app) is yours and must be idempotent; gate
# each producer on reload_active(app).
# ===========================================================================
RELOAD_STATE_ATTR = "_owui_live_reload"  # {key: {active, exec_id, listener}}
RELOAD_EXEC_ID = uuid.uuid4().hex  # new on every exec of this source
# Open WebUI executes each function as module "function_<id>", or on newer
# builds "function_<id>_<32 hex digits>"; the id is what is left of either.
RELOAD_FUNCTION_ID = __name__.removeprefix("function_")
if (
    len(RELOAD_FUNCTION_ID) > 33
    and RELOAD_FUNCTION_ID[-33] == "_"
    and all(c in "0123456789abcdef" for c in RELOAD_FUNCTION_ID[-32:])
):
    RELOAD_FUNCTION_ID = RELOAD_FUNCTION_ID[:-33]

reload_log = logging.getLogger("owui-live-reload")
# asyncio only keeps weak references to tasks.
reload_tasks: set = set()


def reload_state(app: Any) -> dict:
    registry = getattr(app.state, RELOAD_STATE_ATTR, None)
    if not isinstance(registry, dict):
        registry = {}
        app.state.__setattr__(RELOAD_STATE_ATTR, registry)
    return registry.setdefault(ASSET_KEY, {})


def reload_active(app: Any) -> bool:
    return bool(reload_state(app).get("active"))


def reload_channel() -> str:
    from open_webui.env import REDIS_KEY_PREFIX

    return f"{REDIS_KEY_PREFIX}:live-reload:{ASSET_KEY}"


def reload_mark_loaded(app: Any) -> None:
    register(app)
    state = reload_state(app)
    state["active"] = True
    state["exec_id"] = RELOAD_EXEC_ID


async def reload_publish(app: Any, active: bool) -> None:
    redis = getattr(app.state, "redis", None)
    if redis is None:
        return
    try:
        await redis.publish(reload_channel(), JSONCodec.dumps({"active": active}))
    except Exception as e:
        reload_log.warning("[%s] publish failed: %s", ASSET_KEY, type(e).__name__)


async def reload_ensure_listener(app: Any) -> None:
    state = reload_state(app)
    if state.get("listener") is not None:
        return
    redis = getattr(app.state, "redis", None)
    if redis is None:
        return

    from types import SimpleNamespace

    from open_webui.models.functions import Functions
    from open_webui.utils.plugin import (
        get_function_module_from_cache,
        get_functions_cache,
    )

    # Mark before awaiting so a concurrent event() can't double-subscribe.
    state["listener"] = True
    try:
        pubsub = redis.pubsub()
        await pubsub.subscribe(reload_channel())
    except Exception as e:
        state["listener"] = None
        reload_log.warning("[%s] subscribe failed: %s", ASSET_KEY, type(e).__name__)
        return

    async def listen():
        try:
            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                if not JSONCodec.loads(message["data"])["active"]:
                    state["active"] = False
                    continue
                was_active = state.get("active")
                # Enable broadcasts arrive before is_active commits; bootstrap trusts this flag.
                state["active"] = True
                context = SimpleNamespace(app=app)
                if not was_active:
                    # Force a fresh exec so its bootstrap registers again.
                    get_functions_cache(context).pop(RELOAD_FUNCTION_ID, None)
                try:
                    function_module, _, _ = await get_function_module_from_cache(
                        context, RELOAD_FUNCTION_ID
                    )
                    # Peers never see a valves save; re-read them like Open WebUI's dispatch does.
                    if hasattr(function_module, "Valves"):
                        valves = await Functions.get_function_valves_by_id(
                            RELOAD_FUNCTION_ID
                        )
                        function_module.valves = function_module.Valves(
                            **(valves or {})
                        )
                except Exception as e:
                    reload_log.warning(
                        "[%s] reload from db failed: %s", ASSET_KEY, type(e).__name__
                    )
        except Exception as e:
            reload_log.warning("[%s] listener stopped: %s", ASSET_KEY, type(e).__name__)

    state["listener"] = asyncio.create_task(listen())


def reload_bootstrap() -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return

    async def bootstrap():
        try:
            from open_webui.main import app
            from open_webui.models.functions import Functions

            own = await Functions.get_function_by_id(RELOAD_FUNCTION_ID)
            # Loading a disabled function (e.g. for its valves) must not switch it on.
            if own is None or not (own.is_active or reload_active(app)):
                return
            reload_mark_loaded(app)
            await reload_ensure_listener(app)
            await reload_publish(app, True)
        except Exception as e:
            reload_log.warning("[%s] bootstrap failed: %s", ASSET_KEY, type(e).__name__)

    task = loop.create_task(bootstrap())
    reload_tasks.add(task)
    task.add_done_callback(reload_tasks.discard)


async def reload_on_event(
    app: Any, event: Optional[dict], event_name: Optional[str]
) -> None:
    await reload_ensure_listener(app)
    state = reload_state(app)

    is_own = ((event or {}).get("subject") or {}).get("id") == RELOAD_FUNCTION_ID
    if is_own and event_name == "function.disable_started":
        state["active"] = False
        await reload_publish(app, False)
        return
    if is_own and event_name == "function.valves_updated":
        await reload_publish(app, True)

    current = state.get("active") and state.get("exec_id") == RELOAD_EXEC_ID
    if current and event_name != "system.startup.completed":
        return
    reload_mark_loaded(app)
    await reload_publish(app, True)


# =========================== end live reload block =========================


import hashlib
from pathlib import Path
from typing import Any

# ===========================================================================
# Shared static-asset registry
# --- KEEP BYTE-IDENTICAL IN EVERY PLUGIN THAT USES IT ----------------------
# ---------------------------------------------------------------------------
# Each plugin publishes a fragment into one app.state registry and the route
# composes them per request, so load order is irrelevant. Per-process: each
# container serves what it has loaded.
#
# Contract:
#   * ASSET_REGISTRY_ATTR, ASSET_ROUTE_ATTR, the entry shape and the paths are
#     the interop surface. The route owner's copy serves everyone.
#   * `key` must be a module-level constant, or a re-exec adds a second entry.
#   * Lower `order` composes first (default 0). Never encode priority in the key.
#   * Producers run on the event loop for every request, 304s included: return
#     a prebuilt string, no I/O, no locks.
#   * Return "" to withdraw. A deleted plugin never sees its own deletion, so
#     disable it first.
#   * Inline fragments only: <script src> / @import lands after hydration.
# ===========================================================================
LOADER_PATH = "/static/loader.js"
CUSTOM_CSS_PATH = "/static/custom.css"
SHARED_ASSET_TYPES = {
    LOADER_PATH: "application/javascript; charset=utf-8",
    CUSTOM_CSS_PATH: "text/css; charset=utf-8",
}
ASSET_REGISTRY_ATTR = "_owui_static_fragments"  # {path: {key: entry}}
ASSET_ROUTE_ATTR = "_owui_shared_asset"  # set to the path the route serves
ASSET_IMPL_ATTR = "_owui_shared_asset_impl"  # implementation version of the route
# Bump on any behaviour change: the newest copy takes over the route.
ASSET_IMPL_VERSION = 4

# Warn once per (path, key, exception type): compose runs on every page load.
_ASSET_WARNED: set = set()


def asset_fragments(app: Any, path: str) -> dict:
    registry = getattr(app.state, ASSET_REGISTRY_ATTR, None)
    if not isinstance(registry, dict):
        registry = {}
        app.state.__setattr__(ASSET_REGISTRY_ATTR, registry)
    bucket = registry.get(path)
    if not isinstance(bucket, dict):
        bucket = {}
        registry[path] = bucket
    return bucket


def asset_sort_key(item):
    """(order, key); a non-int order falls back to 0 so it cannot break sorted()."""
    key, entry = item
    try:
        order = int(entry.get("order", 0))
    except (TypeError, ValueError):
        order = 0
    return (order, key)


def asset_strip_block(content: str, start_marker: str, end_marker: str) -> str:
    """Remove every marker-wrapped block, leaving other content untouched."""
    while start_marker in content:
        start = content.find(start_marker)
        end = content.find(end_marker, start)
        if end == -1:
            # No end marker: the block was appended last, so drop to EOF.
            content = content[:start]
            break
        end += len(end_marker)
        if content[end : end + 1] == "\n":
            end += 1
        content = content[:start] + content[end:]
    return content


def asset_compose(app: Any, path: str) -> str:
    """Disk file plus every registered fragment, in (order, key) order."""
    import logging

    try:
        from open_webui.env import STATIC_DIR

        target = Path(STATIC_DIR) / path.rsplit("/", 1)[-1]
        body = (
            ""
            if (target.is_symlink() or not target.is_file())
            else target.read_text(encoding="utf-8")
        )
    except Exception:
        body = ""

    ordered = sorted(asset_fragments(app, path).items(), key=asset_sort_key)
    # Strip first: an older file-writing build may have left a block on disk.
    for _key, entry in ordered:
        body = asset_strip_block(body, entry["start"], entry["end"])
    body = body.rstrip()

    for key, entry in ordered:
        try:
            block = (entry["js"]() or "").strip()
        except Exception as exc:
            mark = (path, key, type(exc).__name__)
            if mark not in _ASSET_WARNED:
                if len(_ASSET_WARNED) > 256:
                    _ASSET_WARNED.clear()
                _ASSET_WARNED.add(mark)
                logging.getLogger("owui-shared-assets").warning(
                    "fragment %r failed for %s - it will be omitted",
                    key,
                    path,
                    exc_info=True,
                )
            continue
        if block:
            body = (body + "\n\n" if body else "") + block
    return body + "\n" if body else ""


def asset_register(
    app: Any, path: str, key: str, start: str, end: str, producer, order: int = 0
) -> None:
    """Publish a fragment and ensure the route exists. Idempotent."""
    from starlette.responses import Response
    from starlette.routing import Mount, Route

    asset_fragments(app, path)[key] = {
        "start": start,
        "end": end,
        "js": producer,
        "order": order,
    }

    for existing in app.routes:
        if getattr(existing, ASSET_ROUTE_ATTR, None) != path:
            continue
        if getattr(existing, ASSET_IMPL_ATTR, 0) >= ASSET_IMPL_VERSION:
            return  # an equal or newer implementation already owns the route
        break  # ours is newer - fall through and replace it

    # Fragments live on app.state, so replacing the route loses nothing.
    app.routes[:] = [r for r in app.routes if getattr(r, "path", "") != path]
    media_type = SHARED_ASSET_TYPES.get(path, "text/plain; charset=utf-8")

    async def serve_asset(request):
        content = asset_compose(app, path)
        etag = (
            '"owui-'
            # Cache validator only; a bare md5() raises on FIPS hosts.
            + hashlib.md5(
                (path + "\x00" + content).encode("utf-8"), usedforsecurity=False
            ).hexdigest()
            + '"'
        )
        # no-cache, not no-store: no-store would disable the 304 below.
        headers = {
            "Cache-Control": "no-cache, must-revalidate, private",
            "ETag": etag,
        }
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers=headers)
        # Starlette only adds charset for text/*.
        return Response(content, media_type=media_type, headers=headers)

    insert_at = len(app.routes)
    for position, existing in enumerate(app.routes):
        if isinstance(existing, Mount) and getattr(existing, "name", "") == "static":
            insert_at = position
            break
    shared = Route(path, serve_asset, methods=["GET"])
    setattr(shared, ASSET_ROUTE_ATTR, path)
    setattr(shared, ASSET_IMPL_ATTR, ASSET_IMPL_VERSION)
    app.routes.insert(insert_at, shared)


# =========================== end shared asset block ========================


STYLE_NAMES = {
    "default": "Default",
    "cursor": "Blinking Cursor",
    "dot": "Pulsing Dot",
    "bars": "Legacy Bars",
    "block": "Terminal Block",
    "typing": "Typing Dots",
    "wave": "Equalizer",
    "spinner": "Spinner",
    "shimmer": "Shimmer Lines",
    "ellipsis": "Ellipsis",
    "scanner": "Scanner",
    "orbit": "Orbit",
    "ripple": "Ripple",
    "breathe": "Breathing Orb",
    "grid": "Dot Grid",
    "flip": "Flipping Square",
    "sparkle": "Sparkle",
    "heartbeat": "Heartbeat",
    "ball": "Bouncing Ball",
}

# The Thinking header icon is small and inline, so it only offers the compact
# styles (no full-width bars/shimmer, no text cursor), plus "none" to hide it.
THINK_NAMES = {
    "default": "Default",
    "none": "Hidden",
    "spinner": "Spinner",
    "dot": "Pulsing Dot",
    "block": "Terminal Block",
    "typing": "Typing Dots",
    "wave": "Equalizer",
    "ellipsis": "Ellipsis",
    "scanner": "Scanner",
    "orbit": "Orbit",
    "ripple": "Ripple",
    "breathe": "Breathing Orb",
    "grid": "Dot Grid",
    "flip": "Flipping Square",
    "sparkle": "Sparkle",
    "heartbeat": "Heartbeat",
    "ball": "Bouncing Ball",
}

# CSS display of each compact indicator element.
DISPLAY = {
    "block": "inline-block",
    "typing": "inline-flex",
    "wave": "inline-flex",
    "spinner": "inline-block",
    "dot": "inline-flex",
    "ellipsis": "inline-flex",
    "scanner": "inline-block",
    "orbit": "inline-block",
    "ripple": "inline-block",
    "breathe": "inline-block",
    "grid": "inline-grid",
    "flip": "inline-block",
    "sparkle": "inline-block",
    "heartbeat": "inline-block",
    "ball": "inline-block",
}
# Styles added in 1.2.0; their CSS is generated below instead of hand-listed.
NEW_STYLES = [
    "ellipsis",
    "scanner",
    "orbit",
    "ripple",
    "breathe",
    "grid",
    "flip",
    "sparkle",
    "heartbeat",
    "ball",
]

OSK_CSS_BASE = r"""
.osk,.osk>span{display:none}
html[data-osk="bars"] #response-content-container>div:not(:first-child) .osk-host[data-osk-kind="cursor"],
html[data-osk="dot"] #response-content-container>div:not(:first-child) .osk-host[data-osk-kind="cursor"],
html[data-osk="shimmer"] #response-content-container>div:not(:first-child) .osk-host[data-osk-kind="cursor"]{display:none!important}
html[data-osk="bars"] .osk-host,
html[data-osk="block"] .osk-host,
html[data-osk="typing"] .osk-host,
html[data-osk="wave"] .osk-host,
html[data-osk="spinner"] .osk-host,
html[data-osk="shimmer"] .osk-host,
html[data-osk="dot"] .osk-host[data-osk-kind="cursor"],
html[data-osk="cursor"] .osk-host[data-osk-kind="dot"]{background:none!important;animation:none!important;opacity:1!important;margin:0!important;width:auto!important;height:auto!important;vertical-align:baseline!important}
html[data-osk="bars"] .osk-host,html[data-osk="shimmer"] .osk-host{display:block!important;width:100%!important}
html[data-osk="block"] .osk-host,html[data-osk="typing"] .osk-host,html[data-osk="wave"] .osk-host,html[data-osk="spinner"] .osk-host{display:inline-block!important}
html[data-osk="dot"] .osk-host[data-osk-kind="cursor"]{display:inline-flex!important}
html[data-osk="cursor"] .osk-host[data-osk-kind="dot"]{display:inline-block!important}
html[data-osk="bars"] .osk-host[data-osk-kind="dot"]>span:not(.osk),
html[data-osk="cursor"] .osk-host[data-osk-kind="dot"]>span:not(.osk),
html[data-osk="block"] .osk-host[data-osk-kind="dot"]>span:not(.osk),
html[data-osk="typing"] .osk-host[data-osk-kind="dot"]>span:not(.osk),
html[data-osk="wave"] .osk-host[data-osk-kind="dot"]>span:not(.osk),
html[data-osk="spinner"] .osk-host[data-osk-kind="dot"]>span:not(.osk),
html[data-osk="shimmer"] .osk-host[data-osk-kind="dot"]>span:not(.osk){display:none!important}
html[data-osk="bars"] .osk-host>.osk,html[data-osk="bars"] .osk>.osk-bars{display:block}
html[data-osk="shimmer"] .osk-host>.osk,html[data-osk="shimmer"] .osk>.osk-shimmer{display:block}
html[data-osk="block"] .osk-host>.osk,html[data-osk="block"] .osk>.osk-block{display:inline-block}
html[data-osk="spinner"] .osk-host>.osk,html[data-osk="spinner"] .osk>.osk-spinner{display:inline-block}
html[data-osk="typing"] .osk-host>.osk,html[data-osk="wave"] .osk-host>.osk{display:inline-block}
html[data-osk="typing"] .osk>.osk-typing,html[data-osk="wave"] .osk>.osk-wave{display:inline-flex}
html[data-osk="dot"] .osk-host[data-osk-kind="cursor"]>.osk,html[data-osk="dot"] .osk>.osk-dot{display:inline-flex}
html[data-osk="cursor"] .osk-host[data-osk-kind="dot"]>.osk,html[data-osk="cursor"] .osk>.osk-cursor{display:inline-block}
.osk-bars{width:100%;margin:8px 0}
.osk-bars-in{display:flex;flex-direction:column;gap:8px;animation:osk-pulse 2s cubic-bezier(.4,0,.6,1) infinite}
.osk-b{display:block;height:8px;border-radius:4px;background:var(--color-gray-200,#e5e5e5)}
html.dark .osk-b{background:var(--color-gray-600,#525252)}
.osk-r1{margin-right:3.5rem}
.osk-g3,.osk-g4{display:grid;gap:1rem}
.osk-g3{grid-template-columns:repeat(3,minmax(0,1fr))}
.osk-g4{grid-template-columns:repeat(4,minmax(0,1fr))}
.osk-s2{grid-column:span 2/span 2}
.osk-mr4{margin-right:1rem}
.osk-dot{position:relative;width:12px;height:12px;margin:8px 4px}
.osk-dot-ring{position:absolute;inset:0;border-radius:9999px;background:var(--color-gray-700,#404040);opacity:.75;animation:osk-pulse 2s cubic-bezier(.4,0,.6,1) infinite}
html.dark .osk-dot-ring{background:var(--color-gray-200,#e5e5e5)}
.osk-dot-core{position:relative;display:inline-flex;width:12px;height:12px;border-radius:9999px;background:#000;animation:osk-size 1.5s ease-in-out infinite}
html.dark .osk-dot-core{background:#fff}
.osk-cursor{width:2px;height:14px;margin-left:2px;vertical-align:text-bottom;background:var(--color-gray-400,#a3a3a3);animation:osk-pulse 2s cubic-bezier(.4,0,.6,1) infinite}
html.dark .osk-cursor{background:var(--color-gray-500,#737373)}
.osk-block{width:7px;height:14px;margin-left:2px;vertical-align:text-bottom;background:var(--color-gray-400,#a3a3a3);animation:osk-blink 1s steps(1,end) infinite}
html.dark .osk-block{background:var(--color-gray-500,#737373)}
.osk-typing{align-items:center;gap:3px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-typing>span{display:block;width:5px;height:5px;border-radius:9999px;background:var(--color-gray-400,#a3a3a3);animation:osk-bounce 1.2s ease-in-out infinite}
html.dark .osk-typing>span{background:var(--color-gray-500,#737373)}
.osk-typing>span:nth-child(2){animation-delay:.15s}
.osk-typing>span:nth-child(3){animation-delay:.3s}
.osk-wave{align-items:center;gap:2px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-wave>span{display:block;width:2px;height:100%;border-radius:1px;background:var(--color-gray-400,#a3a3a3);animation:osk-wave 1s ease-in-out infinite}
html.dark .osk-wave>span{background:var(--color-gray-500,#737373)}
.osk-wave>span:nth-child(2){animation-delay:.15s}
.osk-wave>span:nth-child(3){animation-delay:.3s}
.osk-wave>span:nth-child(4){animation-delay:.45s}
.osk-spinner{box-sizing:border-box;width:12px;height:12px;margin-left:4px;vertical-align:text-bottom;border:2px solid var(--color-gray-200,#e5e5e5);border-top-color:var(--color-gray-500,#737373);border-radius:9999px;animation:osk-spin .8s linear infinite}
html.dark .osk-spinner{border-color:var(--color-gray-700,#404040);border-top-color:var(--color-gray-300,#d4d4d4)}
.osk-shimmer{width:100%;margin:8px 0}
.osk-shimmer-in{display:flex;flex-direction:column;gap:8px}
.osk-sh{display:block;height:8px;border-radius:4px;background:linear-gradient(90deg,var(--color-gray-200,#e5e5e5) 30%,var(--color-gray-50,#fafafa) 50%,var(--color-gray-200,#e5e5e5) 70%);background-size:200% 100%;animation:osk-shimmer 1.6s linear infinite}
html.dark .osk-sh{background-image:linear-gradient(90deg,var(--color-gray-700,#404040) 30%,var(--color-gray-500,#737373) 50%,var(--color-gray-700,#404040) 70%)}
.osk-sh:nth-child(2){width:85%}
.osk-sh:nth-child(3){width:60%}
.osk-pv{display:inline-flex;flex-shrink:0;align-items:center;width:56px;height:22px}
.osk-pv>span{display:none}
.osk-pv[data-pv="cursor"]>.osk-cursor,.osk-pv[data-pv="block"]>.osk-block,.osk-pv[data-pv="spinner"]>.osk-spinner{display:inline-block}
.osk-pv[data-pv="dot"]>.osk-dot,.osk-pv[data-pv="typing"]>.osk-typing,.osk-pv[data-pv="wave"]>.osk-wave{display:inline-flex}
.osk-pv[data-pv="bars"]>.osk-bars,.osk-pv[data-pv="shimmer"]>.osk-shimmer{display:block}
.osk-pv .osk-bars,.osk-pv .osk-shimmer{margin:0}
.osk-pv .osk-bars-in,.osk-pv .osk-shimmer-in{gap:3px}
.osk-pv .osk-b,.osk-pv .osk-sh{height:3px;border-radius:2px}
.osk-pv .osk-r1{margin-right:1rem}
.osk-pv .osk-g3,.osk-pv .osk-g4{gap:4px}
.osk-pv .osk-mr4{margin-right:4px}
.osk-pv .osk-dot{margin:0 4px}
.osk-pv .osk-cursor,.osk-pv .osk-block,.osk-pv .osk-typing,.osk-pv .osk-wave,.osk-pv .osk-spinner{margin-left:4px}
.osk-trigger{display:flex;align-items:center;gap:6px}
.osk-chev{width:12px;height:12px;flex-shrink:0}
.osk-menu{position:fixed;z-index:10000;min-width:13rem;max-height:min(24rem,calc(100dvh - 2rem));overflow-y:auto;padding:2px;border:1px solid var(--color-gray-100,#f5f5f5);border-radius:.75rem;background:#fff;color:var(--color-gray-900,#171717);box-shadow:0 10px 15px -3px rgb(0 0 0/.1),0 4px 6px -4px rgb(0 0 0/.1);outline:none}
html.dark .osk-menu{border-color:var(--color-gray-800,#262626);background:var(--color-gray-850,#1c1c1c);color:#fff}
.osk-opt{display:flex;align-items:center;gap:8px;min-height:1.875rem;padding:0 8px;border-radius:.75rem;font-size:.8125rem;cursor:pointer;outline:none}
.osk-opt:hover,.osk-opt:focus-visible{background:color-mix(in oklab,var(--color-gray-100,#f5f5f5) 70%,transparent)}
html.dark .osk-opt:hover,html.dark .osk-opt:focus-visible{background:color-mix(in oklab,var(--color-gray-800,#262626) 60%,transparent)}
.osk-opt-name{flex:1;min-width:0;white-space:nowrap}
.osk-opt-check{width:14px;height:14px;flex-shrink:0;visibility:hidden}
.osk-opt[aria-selected="true"] .osk-opt-check{visibility:visible}
@keyframes osk-pulse{50%{opacity:.5}}
@keyframes osk-size{0%,100%{transform:scale(1)}50%{transform:scale(1.25)}}
@keyframes osk-blink{50%{opacity:0}}
@keyframes osk-bounce{0%,60%,100%{transform:translateY(0);opacity:.5}30%{transform:translateY(-3px);opacity:1}}
@keyframes osk-wave{0%,100%{transform:scaleY(.35)}50%{transform:scaleY(1)}}
@keyframes osk-spin{to{transform:rotate(360deg)}}
@keyframes osk-shimmer{from{background-position:100% 0}to{background-position:-100% 0}}
"""

OSK_CSS_NEW = r"""
:root{--osk-c:var(--color-gray-400,#a3a3a3);--osk-m:var(--color-gray-500,#737373);--osk-t:var(--color-gray-200,#e5e5e5);--osk-s:var(--color-gray-700,#404040)}
html.dark{--osk-c:var(--color-gray-500,#737373);--osk-m:var(--color-gray-400,#a3a3a3);--osk-t:var(--color-gray-700,#404040);--osk-s:var(--color-gray-200,#e5e5e5)}
.osk-ellipsis{align-items:center;gap:3px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-ellipsis>span{display:block;width:4px;height:4px;border-radius:9999px;background:var(--osk-m);animation:osk-seq 1.4s ease-in-out infinite}
.osk-ellipsis>span:nth-child(2){animation-delay:.2s}
.osk-ellipsis>span:nth-child(3){animation-delay:.4s}
.osk-scanner{position:relative;width:36px;height:4px;margin-left:4px;vertical-align:middle;overflow:hidden;border-radius:2px;background:var(--osk-t)}
.osk-scanner>span{position:absolute;top:0;left:0;width:40%;height:100%;border-radius:2px;background:var(--osk-m);animation:osk-scan 1.1s ease-in-out infinite alternate}
.osk-orbit{position:relative;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom;animation:osk-spin 1.1s linear infinite}
.osk-orbit>span{position:absolute;left:50%;width:5px;height:5px;margin-left:-2.5px;border-radius:9999px;background:var(--osk-m)}
.osk-orbit>span:first-child{top:0}
.osk-orbit>span:last-child{bottom:0;opacity:.45}
.osk-ripple{position:relative;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom}
.osk-ripple>span{position:absolute;top:0;right:0;bottom:0;left:0;box-sizing:border-box;border:2px solid var(--osk-m);border-radius:9999px;opacity:0;animation:osk-ripple 1.6s ease-out infinite}
.osk-ripple>span:nth-child(2){animation-delay:.8s}
.osk-breathe{width:10px;height:10px;margin:0 5px;vertical-align:middle;border-radius:9999px;background:var(--osk-s);box-shadow:0 0 8px 2px color-mix(in oklab,var(--osk-s) 40%,transparent);animation:osk-breathe 2s ease-in-out infinite}
.osk-grid{grid-template-columns:repeat(3,3px);gap:2px;margin-left:4px;vertical-align:text-bottom}
.osk-grid>span{display:block;width:3px;height:3px;border-radius:1px;background:var(--osk-m);animation:osk-grid 1.2s ease-in-out infinite}
.osk-grid>span:nth-child(2),.osk-grid>span:nth-child(4){animation-delay:.1s}
.osk-grid>span:nth-child(3),.osk-grid>span:nth-child(5),.osk-grid>span:nth-child(7){animation-delay:.2s}
.osk-grid>span:nth-child(6),.osk-grid>span:nth-child(8){animation-delay:.3s}
.osk-grid>span:nth-child(9){animation-delay:.4s}
.osk-flip{width:10px;height:10px;margin:0 5px;vertical-align:middle;border-radius:2px;background:var(--osk-m);animation:osk-flip 1.2s ease-in-out infinite}
.osk-sparkle{width:14px;height:14px;margin:0 3px;vertical-align:text-bottom;color:var(--osk-m);animation:osk-sparkle 1.6s ease-in-out infinite}
.osk-sparkle>svg{display:block;width:100%;height:100%}
.osk-heartbeat{width:28px;height:14px;margin-left:4px;vertical-align:text-bottom;color:var(--osk-m)}
.osk-heartbeat>svg{display:block;width:100%;height:100%;overflow:visible}
.osk-heartbeat path{fill:none;stroke:currentColor;stroke-width:1.5;stroke-linecap:round;stroke-linejoin:round;stroke-dasharray:50;stroke-dashoffset:50;animation:osk-ecg 1.6s linear infinite}
.osk-ball{position:relative;width:12px;height:16px;margin-left:4px;vertical-align:text-bottom}
.osk-ball>span{position:absolute;left:2px;top:0;width:8px;height:8px;border-radius:9999px;background:var(--osk-m);animation:osk-ball .8s infinite}
@keyframes osk-seq{0%,100%{opacity:.15}40%,70%{opacity:1}}
@keyframes osk-scan{from{transform:translateX(0)}to{transform:translateX(150%)}}
@keyframes osk-ripple{0%{transform:scale(.2);opacity:.9}100%{transform:scale(1);opacity:0}}
@keyframes osk-breathe{0%,100%{transform:scale(.65);opacity:.55}50%{transform:scale(1.1);opacity:1}}
@keyframes osk-grid{0%,70%,100%{transform:scale(1);opacity:.3}35%{transform:scale(1.4);opacity:1}}
@keyframes osk-flip{0%{transform:perspective(60px) rotateX(0) rotateY(0)}50%{transform:perspective(60px) rotateX(-180.1deg) rotateY(0)}100%{transform:perspective(60px) rotateX(-180deg) rotateY(-179.9deg)}}
@keyframes osk-sparkle{0%,100%{transform:scale(.6) rotate(0);opacity:.5}50%{transform:scale(1.1) rotate(90deg);opacity:1}}
@keyframes osk-ecg{0%{stroke-dashoffset:50;opacity:1}55%{stroke-dashoffset:0;opacity:1}85%{stroke-dashoffset:0;opacity:0}100%{stroke-dashoffset:50;opacity:0}}
@keyframes osk-ball{0%{transform:translateY(0);animation-timing-function:ease-in}50%{transform:translateY(8px) scale(1.15,.85);animation-timing-function:ease-out}100%{transform:translateY(0)}}
"""


def _build_generated_css() -> str:
    """Selector lists for every style added in 1.2.0, plus the Thinking icon.

    Generated so adding a style is one entry in STYLE_NAMES, DISPLAY and
    NEW_STYLES instead of hand-editing a dozen long selector lists.
    """

    def join(template: str, keys) -> str:
        return ",".join(template.format(k=k) for k in keys)

    rules = [
        # Message hosts: strip Open WebUI's own cursor/dot look for new styles.
        join('html[data-osk="{k}"] .osk-host', NEW_STYLES)
        + "{background:none!important;animation:none!important;opacity:1!important;"
        "margin:0!important;width:auto!important;height:auto!important;"
        "vertical-align:baseline!important;display:inline-block!important}",
        join(
            'html[data-osk="{k}"] .osk-host[data-osk-kind="dot"]>span:not(.osk)',
            NEW_STYLES,
        )
        + "{display:none!important}",
    ]
    for k in NEW_STYLES:
        rules.append(f'html[data-osk="{k}"] .osk-host>.osk{{display:inline-block}}')
        rules.append(f'html[data-osk="{k}"] .osk>.osk-{k}{{display:{DISPLAY[k]}}}')
        rules.append(f'.osk-pv[data-pv="{k}"]>.osk-{k}{{display:{DISPLAY[k]}}}')

    # Thinking icon: our element sits next to Open WebUI's spinner svg and
    # takes over when a non-default style is chosen.
    rules += [
        ".osk-th{display:none}",
        ".osk-th>span{display:none;margin:0}",
        "html[data-osk-think] .osk-th{display:inline-flex;align-items:center;"
        "justify-content:center;flex-shrink:0;min-width:16px;height:16px}",
        'html[data-osk-think="none"] .osk-th{display:none}',
        "html[data-osk-think] svg[data-osk-n]{display:none!important}",
    ]
    for k, display in DISPLAY.items():
        rules.append(
            f'html[data-osk-think="{k}"] .osk-th>.osk-{k}{{display:{display}}}'
        )
    return "\n".join(rules)


OSK_CSS = "\n".join([OSK_CSS_BASE.strip(), OSK_CSS_NEW.strip(), _build_generated_css()])

LOADER_SCRIPT = r"""
(function () {
  'use strict';
  if (window.__owuiMessageIndicator) return;
  window.__owuiMessageIndicator = true;

  var CFG = __CONFIG__;
  var CSS = __CSS__;
  var ROOT = document.documentElement;
  var MARKUP =
    '<span class="osk-bars"><span class="osk-bars-in">' +
    '<span class="osk-b osk-r1"></span>' +
    '<span class="osk-g3"><span class="osk-b osk-s2"></span><span class="osk-b"></span></span>' +
    '<span class="osk-g4"><span class="osk-b"></span><span class="osk-b osk-s2"></span><span class="osk-b osk-mr4"></span></span>' +
    '<span class="osk-b"></span>' +
    '</span></span>' +
    '<span class="osk-dot"><span class="osk-dot-ring"></span><span class="osk-dot-core"></span></span>' +
    '<span class="osk-cursor"></span>' +
    '<span class="osk-block"></span>' +
    '<span class="osk-typing"><span></span><span></span><span></span></span>' +
    '<span class="osk-wave"><span></span><span></span><span></span><span></span></span>' +
    '<span class="osk-spinner"></span>' +
    '<span class="osk-shimmer"><span class="osk-shimmer-in">' +
    '<span class="osk-sh"></span><span class="osk-sh"></span><span class="osk-sh"></span>' +
    '</span></span>' +
    '<span class="osk-ellipsis"><span></span><span></span><span></span></span>' +
    '<span class="osk-scanner"><span></span></span>' +
    '<span class="osk-orbit"><span></span><span></span></span>' +
    '<span class="osk-ripple"><span></span><span></span></span>' +
    '<span class="osk-breathe"></span>' +
    '<span class="osk-grid"><span></span><span></span><span></span><span></span><span></span><span></span><span></span><span></span><span></span></span>' +
    '<span class="osk-flip"></span>' +
    '<span class="osk-sparkle"><svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M12 0c.6 6.6 4.8 11.4 12 12-7.2.6-11.4 5.4-12 12C11.4 17.4 6.6 12.6 0 12 6.6 11.4 11.4 6.6 12 0z"/></svg></span>' +
    '<span class="osk-heartbeat"><svg viewBox="0 0 28 14" aria-hidden="true"><path pathLength="50" d="M0 7h7l2-4 3 8 2-10 3 8 2-2h9"/></svg></span>' +
    '<span class="osk-ball"><span></span></span>';
  var CHEVRON =
    '<svg class="osk-chev" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2.5" stroke="currentColor" aria-hidden="true">' +
    '<path stroke-linecap="round" stroke-linejoin="round" d="m19.5 8.25-7.5 7.5-7.5-7.5"/></svg>';
  var CHECK =
    '<svg class="osk-opt-check" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" aria-hidden="true">' +
    '<path stroke-linecap="round" stroke-linejoin="round" d="m4.5 12.75 6 6 9-13.5"/></svg>';
  var scheduled = false;
  var openPicker = null;

  // One picker = one setting row (trigger + dropdown) bound to one localStorage key
  // and one attribute on <html>. The message and thinking settings share this code.
  function makePicker(spec) {
    var NAMES = spec.names;
    var ORDER = Object.keys(NAMES);
    var trigger = null;
    var menu = null;
    var self = {};

    function choice() {
      var value = null;
      try { value = localStorage.getItem(spec.key); } catch (e) {}
      return ORDER.indexOf(value) === -1 ? 'default' : value;
    }

    function effective() {
      var value = choice();
      return value === 'default' ? spec.fallback : value;
    }

    function apply() {
      var value = effective();
      if (value === 'default') ROOT.removeAttribute(spec.attr);
      else ROOT.setAttribute(spec.attr, value);
    }

    function label(value) {
      if (value !== 'default' || spec.fallback === 'default') return NAMES[value];
      return NAMES.default + ' (' + (NAMES[spec.fallback] || spec.fallback) + ')';
    }

    function preview(value) {
      var shown = value === 'default' ? spec.fallback : value;
      if (shown === 'default' && spec.defaultPreview) shown = spec.defaultPreview;
      var el = document.createElement('span');
      el.className = 'osk-pv';
      el.setAttribute('aria-hidden', 'true');
      if (shown !== 'default' && shown !== 'none') {
        el.setAttribute('data-pv', shown);
        el.innerHTML = MARKUP;
      }
      return el;
    }

    function renderTrigger() {
      if (!trigger) return;
      var value = choice();
      var mode = document.createElement('span');
      mode.id = spec.id + '-mode';
      mode.textContent = label(value);
      trigger.replaceChildren(preview(value), mode);
      trigger.insertAdjacentHTML('beforeend', CHEVRON);
    }

    function placeMenu() {
      if (!menu || !trigger) return;
      var rect = trigger.getBoundingClientRect();
      var gap = 4;
      var pad = 16;
      var height = menu.offsetHeight;
      var width = menu.offsetWidth;
      var below = window.innerHeight - rect.bottom - gap;
      var above = rect.top - gap;
      var top = below < height && above > below ? rect.top - gap - height : rect.bottom + gap;
      var left = Math.min(rect.right - width, window.innerWidth - width - pad);
      menu.style.top = Math.max(pad, top) + 'px';
      menu.style.left = Math.max(pad, left) + 'px';
    }

    function closeMenu(refocus) {
      if (!menu) return;
      menu.remove();
      menu = null;
      if (openPicker === self) openPicker = null;
      window.removeEventListener('keydown', onMenuKey, true);
      document.removeEventListener('pointerdown', onOutside, true);
      window.removeEventListener('resize', onResize);
      window.removeEventListener('scroll', placeMenu, true);
      if (!trigger) return;
      trigger.setAttribute('aria-expanded', 'false');
      if (refocus && trigger.isConnected) trigger.focus();
    }

    function pick(value) {
      try { localStorage.setItem(spec.key, value); } catch (e) {}
      apply();
      renderTrigger();
      closeMenu(true);
    }

    function onOutside(evt) {
      if (menu && !menu.contains(evt.target) && !(trigger && trigger.contains(evt.target))) closeMenu(false);
    }

    function onResize() {
      closeMenu(false);
    }

    function onMenuKey(evt) {
      var items = menu ? Array.prototype.slice.call(menu.querySelectorAll('[role="option"]')) : [];
      var at = items.indexOf(document.activeElement);
      var to = -1;
      if (evt.key === 'Escape') closeMenu(true);
      else if (evt.key === 'Tab') { closeMenu(true); return; }
      else if (evt.key === 'ArrowDown') to = at < 0 ? 0 : Math.min(at + 1, items.length - 1);
      else if (evt.key === 'ArrowUp') to = at < 0 ? items.length - 1 : Math.max(at - 1, 0);
      else if (evt.key === 'Home') to = 0;
      else if (evt.key === 'End') to = items.length - 1;
      else if ((evt.key === 'Enter' || evt.key === ' ') && at >= 0) pick(items[at].getAttribute('data-value'));
      else return;
      evt.preventDefault();
      evt.stopPropagation();
      if (to >= 0) items[to].focus();
    }

    function openMenu() {
      if (menu || !trigger) return;
      if (openPicker && openPicker !== self) openPicker.closeMenu(false);
      openPicker = self;
      var current = choice();
      var selected = null;
      menu = document.createElement('div');
      menu.id = spec.id + '-menu';
      menu.className = 'osk-menu';
      menu.tabIndex = -1;
      menu.setAttribute('role', 'listbox');
      menu.setAttribute('aria-labelledby', spec.id + '-label');
      ORDER.forEach(function (value) {
        var item = document.createElement('div');
        item.className = 'osk-opt';
        item.tabIndex = -1;
        item.setAttribute('role', 'option');
        item.setAttribute('data-value', value);
        item.setAttribute('aria-selected', value === current ? 'true' : 'false');
        var name = document.createElement('span');
        name.className = 'osk-opt-name';
        name.textContent = label(value);
        item.appendChild(preview(value));
        item.appendChild(name);
        item.insertAdjacentHTML('beforeend', CHECK);
        item.addEventListener('click', function () { pick(value); });
        menu.appendChild(item);
        if (value === current) selected = item;
      });
      menu.addEventListener('mousedown', function (evt) { evt.stopPropagation(); });
      (trigger.closest('.modal') || document.body).appendChild(menu);
      trigger.setAttribute('aria-expanded', 'true');
      placeMenu();
      window.addEventListener('keydown', onMenuKey, true);
      document.addEventListener('pointerdown', onOutside, true);
      window.addEventListener('resize', onResize);
      window.addEventListener('scroll', placeMenu, true);
      (selected || menu).focus();
    }

    function ensureRow() {
      if (document.getElementById(spec.id)) return;
      var at = spec.place();
      if (!at) return;

      var block = document.createElement('div');
      block.id = spec.id;
      var line = document.createElement('div');
      line.className = at.rowClass || 'flex items-center justify-between gap-2.5';
      var name = document.createElement('div');
      name.id = spec.id + '-label';
      name.className = at.nameClass || 'min-w-0 text-xs text-gray-600 dark:text-gray-400';
      name.textContent = spec.title;
      closeMenu(false);
      trigger = document.createElement('button');
      trigger.type = 'button';
      trigger.className = 'osk-trigger text-xs text-gray-500 transition-colors hover:text-gray-900 dark:text-gray-500 dark:hover:text-white';
      trigger.setAttribute('aria-labelledby', spec.id + '-label ' + spec.id + '-mode');
      trigger.setAttribute('aria-haspopup', 'listbox');
      trigger.setAttribute('aria-expanded', 'false');
      trigger.setAttribute('aria-controls', spec.id + '-menu');
      trigger.addEventListener('click', function () {
        if (menu) closeMenu(false);
        else openMenu();
      });
      trigger.addEventListener('keydown', function (evt) {
        if (menu || (evt.key !== 'ArrowDown' && evt.key !== 'ArrowUp')) return;
        evt.preventDefault();
        openMenu();
      });
      renderTrigger();
      line.appendChild(name);
      line.appendChild(trigger);
      var note = document.createElement('p');
      note.className = 'mt-1.5 text-[0.6875rem] text-gray-400 dark:text-gray-600';
      note.textContent = spec.note;
      block.appendChild(line);
      block.appendChild(note);
      at.parent.insertBefore(block, at.before);
    }

    function check() {
      if (menu && !(menu.isConnected && trigger && trigger.isConnected)) closeMenu(false);
    }

    window.addEventListener('storage', function (evt) {
      if (evt.key !== spec.key) return;
      apply();
      closeMenu(false);
      renderTrigger();
    });

    self.apply = apply;
    self.closeMenu = closeMenu;
    self.ensureRow = ensureRow;
    self.check = check;
    return self;
  }

  var message = makePicker({
    id: 'osk-setting',
    key: 'owui-message-loading-indicator',
    attr: 'data-osk',
    names: CFG.names,
    fallback: CFG.fallback,
    title: 'Message Loading Indicator',
    note: 'How a reply shows that it is still loading. Saved in this browser.',
    place: function () {
      var anchor = document.getElementById('chat-bubble-ui-label');
      if (!anchor || !anchor.closest('#tab-interface')) return null;
      var row = anchor.parentElement;
      var wrapper = row && row.parentElement;
      if (!wrapper || !wrapper.parentElement) return null;
      return {
        parent: wrapper.parentElement,
        before: wrapper.nextSibling,
        rowClass: row.className,
        nameClass: anchor.className
      };
    }
  });

  var think = makePicker({
    id: 'osk-think-setting',
    key: 'owui-thinking-indicator',
    attr: 'data-osk-think',
    names: CFG.thinkNames,
    fallback: CFG.thinkFallback,
    defaultPreview: 'spinner',
    title: 'Thinking Indicator',
    note: 'The icon beside the Thinking header while a model reasons. Saved in this browser.',
    place: function () {
      var prev = document.getElementById('osk-setting');
      if (!prev || !prev.parentElement) return null;
      var label = document.getElementById('osk-setting-label');
      return {
        parent: prev.parentElement,
        before: prev.nextSibling,
        rowClass: prev.firstElementChild ? prev.firstElementChild.className : '',
        nameClass: label ? label.className : ''
      };
    }
  });

  function ensureStyle() {
    if (document.getElementById('owui-osk-style')) return;
    var style = document.createElement('style');
    style.id = 'owui-osk-style';
    style.textContent = CSS;
    (document.head || ROOT).appendChild(style);
  }

  function decorate(host, kind) {
    if (host.classList.contains('osk-host')) return;
    host.classList.add('osk-host');
    host.setAttribute('data-osk-kind', kind);
    var wrap = document.createElement('span');
    wrap.className = 'osk';
    wrap.setAttribute('aria-hidden', 'true');
    wrap.innerHTML = MARKUP;
    host.appendChild(wrap);
  }

  // Open WebUI's Thinking header shows a spinner svg (a <style> block defining
  // .spinner_xxxx keyframes). Add our own element beside it; CSS swaps them.
  function decorateThink(svg) {
    var host = svg.parentElement;
    if (!host || svg.hasAttribute('data-osk-n')) return;
    svg.setAttribute('data-osk-n', '1');
    var el = document.createElement('span');
    el.className = 'osk-th';
    el.setAttribute('aria-hidden', 'true');
    el.innerHTML = MARKUP;
    el._oskSvg = svg;
    host.insertBefore(el, svg.nextSibling);
  }

  function inReply(node) {
    if (node.closest('#response-content-container')) return true;
    for (var el = node.parentElement; el; el = el.parentElement) {
      var first = el.firstElementChild;
      if (first && /^responses-container-/.test(first.id || '')) return true;
    }
    return false;
  }

  function scan() {
    scheduled = false;
    // 0.12.x renamed the cursor animation to animate-cursor-pulse; older builds use animate-pulse.
    var cursors = document.querySelectorAll('span.inline-block.align-text-bottom.animate-cursor-pulse, #response-content-container span.inline-block.animate-pulse.align-text-bottom');
    for (var i = 0; i < cursors.length; i++) if (inReply(cursors[i])) decorate(cursors[i], 'cursor');
    var dots = document.querySelectorAll('span.animate-size');
    for (var j = 0; j < dots.length; j++) {
      var outer = dots[j].parentElement;
      if (outer && outer.tagName === 'SPAN' && outer.classList.contains('relative') && inReply(outer)) decorate(outer, 'dot');
    }
    var spinners = document.querySelectorAll('.shimmer svg, button[aria-expanded] svg');
    for (var k = 0; k < spinners.length; k++) {
      var sheet = spinners[k].querySelector('style');
      if (sheet && sheet.textContent.indexOf('spinner_') !== -1) decorateThink(spinners[k]);
    }
    // The spinner is removed when thinking ends; drop the element we added beside it.
    var added = document.querySelectorAll('.osk-th');
    for (var m = 0; m < added.length; m++) {
      if (!added[m]._oskSvg || !added[m]._oskSvg.isConnected) added[m].remove();
    }
    message.check();
    think.check();
    message.ensureRow();
    think.ensureRow();
  }

  function schedule() {
    if (scheduled) return;
    scheduled = true;
    requestAnimationFrame(scan);
  }

  ensureStyle();
  message.apply();
  think.apply();
  new MutationObserver(schedule).observe(ROOT, { childList: true, subtree: true });
  schedule();
})();
"""


class Event:
    class Valves(BaseModel):
        default_style: Literal[
            "default",
            "cursor",
            "dot",
            "bars",
            "block",
            "typing",
            "wave",
            "spinner",
            "shimmer",
            "ellipsis",
            "scanner",
            "orbit",
            "ripple",
            "breathe",
            "grid",
            "flip",
            "sparkle",
            "heartbeat",
            "ball",
        ] = Field(
            default="default",
            description="What Default means in the Message Loading Indicator setting for users who have not picked a style. 'default' keeps Open WebUI's own indicator. Others: cursor, dot, bars (legacy loading bars), block (terminal block cursor), typing, wave (equalizer), spinner, shimmer (skeleton lines), ellipsis, scanner, orbit, ripple, breathe (breathing orb), grid (dot grid), flip (flipping square), sparkle, heartbeat, ball (bouncing ball).",
        )
        default_thinking_style: Literal[
            "default",
            "none",
            "spinner",
            "dot",
            "block",
            "typing",
            "wave",
            "ellipsis",
            "scanner",
            "orbit",
            "ripple",
            "breathe",
            "grid",
            "flip",
            "sparkle",
            "heartbeat",
            "ball",
        ] = Field(
            default="default",
            description="What Default means in the Thinking Indicator setting, the icon beside the Thinking header while a model reasons. 'default' keeps Open WebUI's own spinner and 'none' hides the icon. The other values match the message styles of the same name.",
        )

    LOADER_BLOCK_START = "// owui-message-loading-indicator:start"
    LOADER_BLOCK_END = "// owui-message-loading-indicator:end"
    ASSET_KEY = "message-loading-indicator"
    ASSET_ORDER = 0

    _fragment_cache = None
    instance = None

    def __init__(self):
        self.valves = self.Valves()
        Event.instance = self
        reload_bootstrap()

    def _loader_fragment(self) -> str:
        cache_key = (self.valves.default_style, self.valves.default_thinking_style)
        cached = Event._fragment_cache
        if cached and cached[0] == cache_key:
            return cached[1]
        config = _json.dumps(
            {
                "fallback": self.valves.default_style,
                "thinkFallback": self.valves.default_thinking_style,
                "names": STYLE_NAMES,
                "thinkNames": THINK_NAMES,
            }
        )
        js = (
            LOADER_SCRIPT.strip()
            .replace("__CONFIG__", config)
            .replace("__CSS__", _json.dumps(OSK_CSS.strip()))
        )
        fragment = f"{self.LOADER_BLOCK_START}\n{js}\n{self.LOADER_BLOCK_END}"
        Event._fragment_cache = (cache_key, fragment)
        return fragment

    def _publish(self, app) -> None:
        asset_register(
            app,
            LOADER_PATH,
            self.ASSET_KEY,
            self.LOADER_BLOCK_START,
            self.LOADER_BLOCK_END,
            lambda: Event.instance._loader_fragment() if reload_active(app) else "",
            order=self.ASSET_ORDER,
        )

    async def event(
        self,
        event: Optional[dict] = None,
        __event_name__: str = "",
        __app__: Any = None,
        **kwargs,
    ) -> None:
        if __app__ is not None:
            await reload_on_event(__app__, event, __event_name__)


def register(app: Any) -> None:
    Event.instance._publish(app)
