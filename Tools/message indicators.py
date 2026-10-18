"""
title: Message Loading Indicator
description: Message Loading Indicator + Thinking Studio. Adds settings under Settings > Interface so each user picks how a reply shows it is loading (100 styles plus a Random option), which icon sits beside the Thinking header, and replaces the bland 'Thinking...' text with rotating thinking words (Claude-style, dev, playful, hacker, zen, Hebrew, custom...) with animated effects, fonts, colors, speed and an optional live timer. Published into /static/loader.js through the Shared Assets Protocol, so it coexists with Theme Designer Pro and any other plugin using it.
author: ichrist (adapted from G30 function)
author_url: https://openwebui.com/u/ichrist
funding_url:
version: 2.2.2
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


# ---------------------------------------------------------------------------
# 1.3.0: 30 more styles. Merged into the dicts above so the generated CSS,
# both pickers and the valves pick them up automatically.
# ---------------------------------------------------------------------------
def _s(n):
    return "<span></span>" * n


MORE_STYLE_NAMES = {
    "dualring": "Dual Ring",
    "chase": "Dot Chase",
    "dna": "DNA Helix",
    "pendulum": "Pendulum",
    "cradle": "Newton's Cradle",
    "progress": "Progress Bar",
    "stripes": "Barber Pole",
    "hourglass": "Hourglass",
    "clock": "Clock",
    "radar": "Radar",
    "pulsering": "Pulse Ring",
    "squares": "Fading Squares",
    "diamond": "Morphing Diamond",
    "sine": "Sine Wave",
    "dotwave": "Dot Wave",
    "chart": "Bar Chart",
    "typewriter": "Typewriter",
    "underscore": "Underscore",
    "lines": "Rolling Lines",
    "pacman": "Chomper",
    "infinity": "Infinity",
    "triangle": "Triangle Trace",
    "morph": "Morphing Blob",
    "dotring": "Dot Ring",
    "bubbles": "Rising Bubbles",
    "stairs": "Stairs",
    "wifi": "Signal",
    "bolt": "Lightning",
    "twinkle": "Twinkle",
    "gradient": "Gradient Ring",
}
# ---- 1.4.0: 40 more styles (merged into MORE_STYLE_NAMES below) ----
EXTRA_STYLE_NAMES = {
    "spiral": "Spiral",
    "target": "Target",
    "squarejump": "Jumping Squares",
    "seesaw": "Seesaw",
    "propeller": "Propeller",
    "cog": "Cog",
    "dotted": "Dotted Circle",
    "ios": "iOS Spinner",
    "comet": "Comet",
    "marquee": "Marquee",
    "flame": "Flame",
    "drop": "Water Drop",
    "rain": "Rain",
    "fill": "Fill Up",
    "battery": "Battery",
    "moon": "Moon Phase",
    "sun": "Sun",
    "heart": "Heart Beat",
    "star": "Star Pulse",
    "spectrum": "Spectrum",
    "bars3": "Triple Bars",
    "swap": "Swap Dots",
    "trio": "Trio Orbit",
    "satellite": "Satellite",
    "atom": "Atom",
    "binary": "Binary",
    "segments": "Segmented Bar",
    "zigzag": "Zigzag",
    "squaretrace": "Square Trace",
    "hexagon": "Hexagon Trace",
    "pingpong": "Ping Pong",
    "traffic": "Traffic Light",
    "search": "Search",
    "matrix": "Dot Matrix",
    "rotsq": "Turning Square",
    "quad": "Quad Spin",
    "knight": "Knight Rider",
    "thought": "Thought Bubble",
    "diamonds": "Diamond Chain",
    "snow": "Snowfall",
}
MORE_STYLE_NAMES.update(EXTRA_STYLE_NAMES)

# ---- 2.1.0: 12 more styles (total 100) ----
V2_STYLE_NAMES = {
    "gyro": "Gyroscope",
    "domino": "Domino Fall",
    "cube": "Cube Spin",
    "blink": "Blinking Eye",
    "metronome": "Metronome",
    "brackets": "Brackets",
    "zipper": "Zipper",
    "firefly": "Fireflies",
    "plus": "Spinning Plus",
    "compass": "Compass",
    "ringpop": "Ring Pop",
    "pulsesq": "Pulse Squares",
}
MORE_STYLE_NAMES.update(V2_STYLE_NAMES)
V2_MARKUP = "".join(
    [
        '<span class="osk-gyro"><span></span><span></span></span>',
        f'<span class="osk-domino">{_s(4)}</span>',
        '<span class="osk-cube"></span>',
        '<span class="osk-blink"></span>',
        '<span class="osk-metronome"><span></span></span>',
        '<span class="osk-brackets"><span></span><span></span></span>',
        f'<span class="osk-zipper">{_s(4)}</span>',
        f'<span class="osk-firefly">{_s(3)}</span>',
        '<span class="osk-plus"></span>',
        '<span class="osk-compass"><span></span></span>',
        '<span class="osk-ringpop"><span></span><span></span></span>',
        f'<span class="osk-pulsesq">{_s(3)}</span>',
    ]
)
V2_CSS = r"""
.osk-gyro{position:relative;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom;perspective:60px}
.osk-gyro>span{position:absolute;inset:0;box-sizing:border-box;border:1.5px solid var(--osk-m);border-radius:9999px}
.osk-gyro>span:nth-child(1){animation:osk-gyro-x 1.6s linear infinite}
.osk-gyro>span:nth-child(2){opacity:.6;animation:osk-gyro-y 1.6s linear infinite}
@keyframes osk-gyro-x{to{transform:rotateX(360deg)}}
@keyframes osk-gyro-y{to{transform:rotateY(360deg)}}
.osk-domino{align-items:flex-end;gap:2px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-domino>span{width:3px;height:12px;border-radius:1px;background:var(--osk-m);transform-origin:bottom center;animation:osk-domino 1.4s ease-in-out infinite}
.osk-domino>span:nth-child(2){animation-delay:.12s}
.osk-domino>span:nth-child(3){animation-delay:.24s}
.osk-domino>span:nth-child(4){animation-delay:.36s}
@keyframes osk-domino{0%,25%{transform:rotate(0)}50%,75%{transform:rotate(62deg)}100%{transform:rotate(0)}}
.osk-cube{width:12px;height:12px;margin-left:4px;vertical-align:text-bottom;border-radius:2px;background:var(--osk-m);animation:osk-cube 1.4s ease-in-out infinite}
@keyframes osk-cube{0%{transform:perspective(40px) rotateY(0)}50%{transform:perspective(40px) rotateY(180deg)}100%{transform:perspective(40px) rotateY(360deg)}}
.osk-blink{position:relative;box-sizing:border-box;width:16px;height:10px;margin-left:4px;vertical-align:middle;border:1.5px solid var(--osk-m);border-radius:50%;animation:osk-blink 2.2s ease-in-out infinite}
.osk-blink::after{content:'';position:absolute;left:50%;top:50%;width:4px;height:4px;margin:-2px 0 0 -2px;border-radius:50%;background:var(--osk-m)}
@keyframes osk-blink{0%,40%,50%,100%{transform:scaleY(1)}45%{transform:scaleY(.1)}}
.osk-metronome{position:relative;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom}
.osk-metronome>span{position:absolute;left:50%;bottom:1px;width:2px;height:14px;margin-left:-1px;border-radius:1px;background:var(--osk-m);transform-origin:bottom center;animation:osk-metro 1s ease-in-out infinite alternate}
.osk-metronome>span::after{content:'';position:absolute;left:-2px;top:0;width:6px;height:4px;border-radius:2px;background:var(--osk-m)}
@keyframes osk-metro{from{transform:rotate(-30deg)}to{transform:rotate(30deg)}}
.osk-brackets{position:relative;width:16px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-brackets>span{position:absolute;top:0;bottom:0;box-sizing:border-box;width:5px;border:2px solid var(--osk-m)}
.osk-brackets>span:nth-child(1){left:0;border-right:0;border-radius:2px 0 0 2px;animation:osk-brk 1.2s ease-in-out infinite alternate}
.osk-brackets>span:nth-child(2){right:0;border-left:0;border-radius:0 2px 2px 0;animation:osk-brk2 1.2s ease-in-out infinite alternate}
@keyframes osk-brk{to{transform:translateX(3px)}}
@keyframes osk-brk2{to{transform:translateX(-3px)}}
.osk-zipper{position:relative;width:22px;height:12px;margin-left:4px;vertical-align:text-bottom}
.osk-zipper>span{position:absolute;top:4px;width:3px;height:3px;border-radius:9999px;background:var(--osk-m);animation:osk-zip 1s ease-in-out infinite}
.osk-zipper>span:nth-child(1){left:0}
.osk-zipper>span:nth-child(2){left:6px;animation-delay:-.5s}
.osk-zipper>span:nth-child(3){left:12px}
.osk-zipper>span:nth-child(4){left:18px;animation-delay:-.5s}
@keyframes osk-zip{0%,100%{transform:translateY(-3px)}50%{transform:translateY(3px)}}
.osk-firefly{position:relative;width:18px;height:16px;margin-left:4px;vertical-align:text-bottom}
.osk-firefly>span{position:absolute;width:3px;height:3px;border-radius:9999px;background:var(--osk-m);box-shadow:0 0 5px var(--osk-m);animation:osk-ff 2.4s ease-in-out infinite}
.osk-firefly>span:nth-child(1){left:2px;top:9px}
.osk-firefly>span:nth-child(2){left:8px;top:3px;animation-delay:-.8s;animation-duration:2.9s}
.osk-firefly>span:nth-child(3){left:14px;top:10px;animation-delay:-1.6s;animation-duration:2.1s}
@keyframes osk-ff{0%,100%{opacity:.15;transform:translate(0,0)}50%{opacity:1;transform:translate(2px,-3px)}}
.osk-plus{position:relative;width:14px;height:14px;margin-left:4px;vertical-align:text-bottom;animation:osk-plus 1.4s ease-in-out infinite}
.osk-plus::before,.osk-plus::after{content:'';position:absolute;border-radius:1px;background:var(--osk-m)}
.osk-plus::before{left:0;right:0;top:6px;height:2px}
.osk-plus::after{top:0;bottom:0;left:6px;width:2px}
@keyframes osk-plus{0%{transform:rotate(0) scale(1)}50%{transform:rotate(90deg) scale(.7)}100%{transform:rotate(180deg) scale(1)}}
.osk-compass{position:relative;box-sizing:border-box;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom;border:1.5px solid var(--osk-m);border-radius:50%}
.osk-compass>span{position:absolute;left:50%;top:50%;width:2px;height:8px;margin:-4px 0 0 -1px;border-radius:1px;background:var(--osk-m);animation:osk-compass 2s ease-in-out infinite}
@keyframes osk-compass{0%,100%{transform:rotate(-40deg)}50%{transform:rotate(60deg)}}
.osk-ringpop{position:relative;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom}
.osk-ringpop>span:nth-child(1){position:absolute;left:50%;top:50%;width:4px;height:4px;margin:-2px 0 0 -2px;border-radius:50%;background:var(--osk-m)}
.osk-ringpop>span:nth-child(2){position:absolute;inset:0;box-sizing:border-box;border:1.5px solid var(--osk-m);border-radius:50%;animation:osk-rp 1.3s ease-out infinite}
@keyframes osk-rp{0%{transform:scale(.25);opacity:1}100%{transform:scale(1);opacity:0}}
.osk-pulsesq{align-items:center;gap:3px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-pulsesq>span{width:4px;height:4px;background:var(--osk-m);animation:osk-psq 1s ease-in-out infinite}
.osk-pulsesq>span:nth-child(2){animation-delay:.15s}
.osk-pulsesq>span:nth-child(3){animation-delay:.3s}
@keyframes osk-psq{0%,100%{transform:scale(.6);opacity:.5}50%{transform:scale(1.6);opacity:1}}
"""

EXTRA_WIDE = {"segments", "pingpong", "knight"}
EXTRA_DISPLAY = {
    k: "inline-flex"
    for k in (
        "squarejump",
        "rain",
        "spectrum",
        "bars3",
        "binary",
        "segments",
        "traffic",
        "thought",
        "diamonds",
        "snow",
    )
}
EXTRA_DISPLAY.update({"matrix": "inline-grid", "quad": "inline-grid"})

_TRACE = 'd="{d}"'


def _trace(cls, vb, d):
    return (
        f'<span class="osk-{cls}"><svg viewBox="{vb}" aria-hidden="true">'
        f'<path class="t" d="{d}"/><path class="a" pathLength="100" d="{d}"/></svg></span>'
    )


EXTRA_MARKUP = "".join(
    [
        f'<span class="osk-spiral">{_s(8)}</span>',
        '<span class="osk-target"></span>',
        f'<span class="osk-squarejump">{_s(3)}</span>',
        '<span class="osk-seesaw"></span>',
        '<span class="osk-propeller"></span>',
        '<span class="osk-cog"></span>',
        '<span class="osk-dotted"></span>',
        f'<span class="osk-ios">{_s(12)}</span>',
        f'<span class="osk-comet">{_s(6)}</span>',
        '<span class="osk-marquee"></span>',
        '<span class="osk-flame"></span>',
        '<span class="osk-drop"></span>',
        f'<span class="osk-rain">{_s(5)}</span>',
        '<span class="osk-fill"></span>',
        '<span class="osk-battery"><span></span></span>',
        '<span class="osk-moon"></span>',
        f'<span class="osk-sun">{_s(8)}</span>',
        '<span class="osk-heart"><svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M12 21s-8-5.2-8-11a4.5 4.5 0 0 1 8-2.8A4.5 4.5 0 0 1 20 10c0 5.8-8 11-8 11z"/></svg></span>',
        '<span class="osk-star"><svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M12 2l2.9 6.9 7.1.6-5.4 4.7 1.7 7.1L12 17.6 5.7 21.3l1.7-7.1L2 9.5l7.1-.6z"/></svg></span>',
        f'<span class="osk-spectrum">{_s(7)}</span>',
        f'<span class="osk-bars3">{_s(3)}</span>',
        f'<span class="osk-swap">{_s(2)}</span>',
        f'<span class="osk-trio">{_s(3)}</span>',
        '<span class="osk-satellite"><span></span></span>',
        f'<span class="osk-atom">{_s(3)}</span>',
        f'<span class="osk-binary">{_s(5)}</span>',
        f'<span class="osk-segments">{_s(6)}</span>',
        '<span class="osk-zigzag"><svg viewBox="0 0 24 12" aria-hidden="true"><path pathLength="50" d="M0 10L4 2L8 10L12 2L16 10L20 2L24 10"/></svg></span>',
        _trace("squaretrace", "0 0 16 16", "M2.5 2.5H13.5V13.5H2.5Z"),
        _trace("hexagon", "0 0 16 16", "M8 1.5L14 5V11L8 14.5L2 11V5Z"),
        '<span class="osk-pingpong"><span></span></span>',
        f'<span class="osk-traffic">{_s(3)}</span>',
        '<span class="osk-search"></span>',
        f'<span class="osk-matrix">{_s(16)}</span>',
        '<span class="osk-rotsq"></span>',
        f'<span class="osk-quad">{_s(4)}</span>',
        '<span class="osk-knight"></span>',
        f'<span class="osk-thought">{_s(3)}</span>',
        f'<span class="osk-diamonds">{_s(3)}</span>',
        f'<span class="osk-snow">{_s(4)}</span>',
    ]
)

# Too wide for the small Thinking header icon.
_WIDE = {"progress", "stripes", "pacman", "typewriter"} | EXTRA_WIDE
MORE_DISPLAY = {k: "inline-block" for k in MORE_STYLE_NAMES}
for _k in ("dna", "dotwave", "chart", "lines", "stairs", "cradle"):
    MORE_DISPLAY[_k] = "inline-flex"
MORE_DISPLAY["squares"] = "inline-grid"
MORE_DISPLAY.update(EXTRA_DISPLAY)
MORE_DISPLAY.update({"domino": "inline-flex", "pulsesq": "inline-flex"})


def _sorted_names(d, first):
    head = {k: d[k] for k in first if k in d}
    rest = sorted(
        ((k, v) for k, v in d.items() if k not in head), key=lambda kv: kv[1].lower()
    )
    return {**head, **dict(rest)}


STYLE_NAMES.update(MORE_STYLE_NAMES)
STYLE_NAMES["random"] = "Random (new each message)"
THINK_NAMES["random"] = "Random (new each time)"
THINK_NAMES.update({k: v for k, v in MORE_STYLE_NAMES.items() if k not in _WIDE})
DISPLAY.update(MORE_DISPLAY)
NEW_STYLES.extend(MORE_STYLE_NAMES)
STYLE_NAMES = _sorted_names(STYLE_NAMES, ("default", "random"))
THINK_NAMES = _sorted_names(THINK_NAMES, ("default", "none", "random"))
STYLE_LITERAL = Literal[tuple(STYLE_NAMES)]
THINK_LITERAL = Literal[tuple(THINK_NAMES)]

MORE_MARKUP = "".join(
    [
        '<span class="osk-dualring"></span>',
        f'<span class="osk-chase">{_s(8)}</span>',
        f'<span class="osk-dna">{_s(5)}</span>',
        '<span class="osk-pendulum"><span></span></span>',
        f'<span class="osk-cradle">{_s(5)}</span>',
        '<span class="osk-progress"><span></span></span>',
        '<span class="osk-stripes"></span>',
        '<span class="osk-hourglass"><svg viewBox="0 0 12 16" aria-hidden="true"><path d="M1 1h10M1 15h10M2 1c0 5 4 5 4 7s-4 2-4 7M10 1c0 5-4 5-4 7s4 2 4 7"/></svg></span>',
        '<span class="osk-clock"></span>',
        '<span class="osk-radar"></span>',
        '<span class="osk-pulsering"></span>',
        f'<span class="osk-squares">{_s(4)}</span>',
        '<span class="osk-diamond"></span>',
        '<span class="osk-sine"><svg viewBox="0 0 32 12" aria-hidden="true"><path pathLength="50" d="M0 6Q4 0 8 6T16 6T24 6T32 6"/></svg></span>',
        f'<span class="osk-dotwave">{_s(5)}</span>',
        f'<span class="osk-chart">{_s(4)}</span>',
        '<span class="osk-typewriter"><span></span></span>',
        '<span class="osk-underscore"></span>',
        f'<span class="osk-lines">{_s(3)}</span>',
        '<span class="osk-pacman"></span>',
        '<span class="osk-infinity"><svg viewBox="0 0 24 12" aria-hidden="true"><path class="t" d="M12 6C9 0 3 0 3 6S9 12 12 6S21 0 21 6S15 12 12 6Z"/><path class="a" pathLength="100" d="M12 6C9 0 3 0 3 6S9 12 12 6S21 0 21 6S15 12 12 6Z"/></svg></span>',
        '<span class="osk-triangle"><svg viewBox="0 0 16 16" aria-hidden="true"><path class="t" d="M8 2L14.5 13.5H1.5Z"/><path class="a" pathLength="100" d="M8 2L14.5 13.5H1.5Z"/></svg></span>',
        '<span class="osk-morph"></span>',
        f'<span class="osk-dotring">{_s(12)}</span>',
        f'<span class="osk-bubbles">{_s(3)}</span>',
        f'<span class="osk-stairs">{_s(4)}</span>',
        '<span class="osk-wifi"><svg viewBox="0 0 16 14" aria-hidden="true"><path d="M1 5Q8 -2 15 5"/><path d="M3.5 7.5Q8 3.5 12.5 7.5"/><path d="M6 10Q8 8.2 10 10"/><circle cx="8" cy="12.5" r="1.2"/></svg></span>',
        '<span class="osk-bolt"><svg viewBox="0 0 16 16" fill="currentColor" aria-hidden="true"><path d="M9.5 0L2 9h5l-1.5 7L14 6.5H9z"/></svg></span>',
        f'<span class="osk-twinkle">{_s(4)}</span>',
        '<span class="osk-gradient"></span>',
        EXTRA_MARKUP,
        V2_MARKUP,
    ]
)


def _ring_css(cls, n, r, d, dur, anim):
    h = d / 2
    out = [
        f".osk-{cls}{{position:relative;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom}}",
        f".osk-{cls}>span{{position:absolute;left:50%;top:50%;width:{d:g}px;height:{d:g}px;margin:-{h:g}px 0 0 -{h:g}px;border-radius:9999px;background:var(--osk-m);animation:{anim} {dur:g}s linear infinite}}",
    ]
    for i in range(n):
        out.append(
            f".osk-{cls}>span:nth-child({i + 1}){{transform:rotate({i * 360 / n:g}deg) translateY(-{r:g}px);animation-delay:-{dur * i / n:.3f}s}}"
        )
    return "\n".join(out)


MORE_CSS_STATIC = r"""
.osk-dualring{position:relative;box-sizing:border-box;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom;border:2px solid transparent;border-top-color:var(--osk-m);border-bottom-color:var(--osk-m);border-radius:9999px;animation:osk-spin 1s linear infinite}
.osk-dualring::after{content:"";position:absolute;top:1px;right:1px;bottom:1px;left:1px;border:2px solid transparent;border-left-color:var(--osk-s);border-right-color:var(--osk-s);border-radius:9999px;animation:osk-spin .7s linear infinite reverse}
.osk-dna{align-items:center;gap:3px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-dna>span{display:block;width:3px;height:3px;border-radius:9999px;background:var(--osk-m);animation:osk-dna 1.2s ease-in-out infinite}
.osk-dna>span:nth-child(even){background:var(--osk-s)}
.osk-dna>span:nth-child(2){animation-delay:-.15s}
.osk-dna>span:nth-child(3){animation-delay:-.3s}
.osk-dna>span:nth-child(4){animation-delay:-.45s}
.osk-dna>span:nth-child(5){animation-delay:-.6s}
.osk-pendulum{position:relative;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom}
.osk-pendulum>span{position:absolute;top:0;left:50%;width:1.5px;height:11px;margin-left:-.75px;background:var(--osk-m);transform-origin:50% 0;animation:osk-swing 1s ease-in-out infinite alternate}
.osk-pendulum>span::after{content:"";position:absolute;left:-2.25px;bottom:-4px;width:6px;height:6px;border-radius:9999px;background:var(--osk-m)}
.osk-cradle{align-items:center;height:14px;margin:0 8px;vertical-align:text-bottom}
.osk-cradle>span{display:block;width:5px;height:5px;border-radius:9999px;background:var(--osk-m)}
.osk-cradle>span:first-child{animation:osk-cla 1.2s ease-in-out infinite}
.osk-cradle>span:last-child{animation:osk-clb 1.2s ease-in-out infinite}
.osk-progress{position:relative;width:48px;height:4px;margin-left:4px;vertical-align:middle;overflow:hidden;border-radius:2px;background:var(--osk-t)}
.osk-progress>span{position:absolute;top:0;left:0;height:100%;border-radius:2px;background:var(--osk-m);animation:osk-prog 1.6s ease-in-out infinite}
.osk-stripes{width:44px;height:6px;margin-left:4px;vertical-align:middle;border-radius:3px;background:repeating-linear-gradient(45deg,var(--osk-m) 0 4px,var(--osk-t) 4px 8px);background-size:11.314px 11.314px;animation:osk-stripe .6s linear infinite}
.osk-hourglass{width:10px;height:14px;margin:0 4px;vertical-align:text-bottom;color:var(--osk-m);animation:osk-hg 2s ease-in-out infinite}
.osk-hourglass>svg{display:block;width:100%;height:100%;fill:none;stroke:currentColor;stroke-width:1.3;stroke-linecap:round}
.osk-clock{position:relative;box-sizing:border-box;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom;border:1.5px solid var(--osk-m);border-radius:9999px}
.osk-clock::before,.osk-clock::after{content:"";position:absolute;left:50%;bottom:50%;width:1.5px;margin-left:-.75px;border-radius:1px;background:var(--osk-m);transform-origin:50% 100%}
.osk-clock::before{height:5px;animation:osk-spin 1s linear infinite}
.osk-clock::after{height:3.5px;animation:osk-spin 12s linear infinite}
.osk-radar{box-sizing:border-box;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom;border:1.5px solid var(--osk-m);border-radius:9999px;background:conic-gradient(from 0deg,transparent 0 250deg,color-mix(in oklab,var(--osk-m) 75%,transparent) 360deg);animation:osk-spin 1.4s linear infinite}
.osk-pulsering{position:relative;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom}
.osk-pulsering::before{content:"";position:absolute;left:50%;top:50%;width:6px;height:6px;margin:-3px 0 0 -3px;border-radius:9999px;background:var(--osk-s);animation:osk-breathe 1.2s ease-in-out infinite}
.osk-pulsering::after{content:"";position:absolute;top:0;right:0;bottom:0;left:0;box-sizing:border-box;border:2px solid var(--osk-m);border-radius:9999px;opacity:0;animation:osk-ripple 1.2s ease-out infinite}
.osk-squares{grid-template-columns:repeat(2,5px);gap:2px;margin-left:4px;vertical-align:text-bottom}
.osk-squares>span{display:block;width:5px;height:5px;border-radius:1px;background:var(--osk-m);animation:osk-sq 1.2s ease-in-out infinite}
.osk-squares>span:nth-child(2){animation-delay:.15s}
.osk-squares>span:nth-child(4){animation-delay:.3s}
.osk-squares>span:nth-child(3){animation-delay:.45s}
.osk-diamond{width:9px;height:9px;margin:0 5px;vertical-align:middle;background:var(--osk-m);animation:osk-dia 1.6s ease-in-out infinite}
.osk-sine{width:28px;height:12px;margin-left:4px;vertical-align:text-bottom;color:var(--osk-m)}
.osk-sine>svg{display:block;width:100%;height:100%;overflow:visible}
.osk-sine path{fill:none;stroke:currentColor;stroke-width:1.5;stroke-linecap:round;stroke-dasharray:50;stroke-dashoffset:50;animation:osk-ecg 1.6s linear infinite}
.osk-dotwave{align-items:center;gap:3px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-dotwave>span{display:block;width:4px;height:4px;border-radius:9999px;background:var(--osk-m);animation:osk-dw 1s ease-in-out infinite}
.osk-dotwave>span:nth-child(2){animation-delay:-.12s}
.osk-dotwave>span:nth-child(3){animation-delay:-.24s}
.osk-dotwave>span:nth-child(4){animation-delay:-.36s}
.osk-dotwave>span:nth-child(5){animation-delay:-.48s}
.osk-chart{align-items:flex-end;gap:2px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-chart>span{display:block;width:3px;height:100%;border-radius:1px;background:var(--osk-m);transform-origin:50% 100%;animation:osk-bar 1.4s ease-in-out infinite}
.osk-chart>span:nth-child(2){animation-delay:-.35s}
.osk-chart>span:nth-child(3){animation-delay:-.7s}
.osk-chart>span:nth-child(4){animation-delay:-1.05s}
.osk-typewriter{width:48px;height:12px;margin-left:4px;vertical-align:text-bottom}
.osk-typewriter>span{display:block;width:0;height:100%;box-sizing:border-box;border-right:2px solid var(--osk-s);background:repeating-linear-gradient(90deg,var(--osk-m) 0 4px,transparent 4px 6px) 0 50%/100% 3px no-repeat;animation:osk-type 2s steps(8) infinite}
.osk-underscore{width:8px;height:2px;margin-left:3px;vertical-align:baseline;background:var(--osk-m);animation:osk-blink 1s steps(1,end) infinite}
.osk-lines{flex-direction:column;justify-content:center;gap:3px;width:22px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-lines>span{display:block;height:2px;border-radius:1px;background:var(--osk-m);transform-origin:0 50%;animation:osk-roll 1.4s ease-in-out infinite}
.osk-lines>span:nth-child(2){animation-delay:-.25s}
.osk-lines>span:nth-child(3){animation-delay:-.5s}
.osk-pacman{position:relative;width:40px;height:14px;margin-left:4px;vertical-align:text-bottom;overflow:hidden}
.osk-pacman::before{content:"";position:absolute;left:0;top:0;width:14px;height:14px;border-radius:9999px;background:var(--osk-m);animation:osk-chomp .5s ease-in-out infinite alternate}
.osk-pacman::after{content:"";position:absolute;top:50%;left:18px;width:calc(100% + 8px);height:3px;margin-top:-1.5px;background:radial-gradient(circle,var(--osk-m) 1.4px,transparent 1.6px) 0 0/8px 3px repeat-x;animation:osk-dots .5s linear infinite}
.osk-infinity{width:24px;height:12px;margin-left:4px;vertical-align:text-bottom;color:var(--osk-m)}
.osk-triangle{width:16px;height:16px;margin:0 3px;vertical-align:text-bottom;color:var(--osk-m)}
.osk-infinity>svg,.osk-triangle>svg{display:block;width:100%;height:100%;overflow:visible}
.osk-infinity path,.osk-triangle path{fill:none;stroke:currentColor;stroke-width:1.6;stroke-linecap:round;stroke-linejoin:round}
.osk-infinity .t,.osk-triangle .t{opacity:.25}
.osk-infinity .a,.osk-triangle .a{stroke-dasharray:30 70;animation:osk-dash 1.4s linear infinite}
.osk-morph{width:12px;height:12px;margin:0 5px;vertical-align:middle;background:var(--osk-m);border-radius:60% 40% 30% 70%/60% 30% 70% 40%;animation:osk-morph 2.4s ease-in-out infinite}
.osk-bubbles{position:relative;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom}
.osk-bubbles>span{position:absolute;bottom:0;box-sizing:border-box;border:1.5px solid var(--osk-m);border-radius:9999px;opacity:0;animation:osk-rise 1.8s ease-in infinite}
.osk-bubbles>span:nth-child(1){left:1px;width:5px;height:5px}
.osk-bubbles>span:nth-child(2){left:6px;width:4px;height:4px;animation-delay:.6s}
.osk-bubbles>span:nth-child(3){left:10px;width:5px;height:5px;animation-delay:1.2s}
.osk-stairs{align-items:flex-end;gap:2px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-stairs>span{display:block;width:3px;border-radius:1px;background:var(--osk-m);animation:osk-seq 1.2s ease-in-out infinite}
.osk-stairs>span:nth-child(1){height:4px}
.osk-stairs>span:nth-child(2){height:7px;animation-delay:.15s}
.osk-stairs>span:nth-child(3){height:10px;animation-delay:.3s}
.osk-stairs>span:nth-child(4){height:14px;animation-delay:.45s}
.osk-wifi{width:16px;height:14px;margin:0 3px;vertical-align:text-bottom;color:var(--osk-m)}
.osk-wifi>svg{display:block;width:100%;height:100%;overflow:visible}
.osk-wifi path{fill:none;stroke:currentColor;stroke-width:1.5;stroke-linecap:round;animation:osk-seq 1.4s ease-in-out infinite}
.osk-wifi path:nth-child(1){animation-delay:.4s}
.osk-wifi path:nth-child(2){animation-delay:.2s}
.osk-wifi circle{fill:currentColor}
.osk-bolt{width:14px;height:14px;margin:0 3px;vertical-align:text-bottom;color:var(--osk-m);animation:osk-bolt 1.4s linear infinite}
.osk-bolt>svg{display:block;width:100%;height:100%}
.osk-twinkle{position:relative;width:18px;height:16px;margin-left:4px;vertical-align:text-bottom}
.osk-twinkle>span{position:absolute;background:var(--osk-m);clip-path:polygon(50% 0,62% 38%,100% 50%,62% 62%,50% 100%,38% 62%,0 50%,38% 38%);animation:osk-tw 1.4s ease-in-out infinite}
.osk-twinkle>span:nth-child(1){left:1px;top:8px;width:5px;height:5px}
.osk-twinkle>span:nth-child(2){left:7px;top:0;width:6px;height:6px;animation-delay:.35s}
.osk-twinkle>span:nth-child(3){left:13px;top:8px;width:4px;height:4px;animation-delay:.7s}
.osk-twinkle>span:nth-child(4){left:6px;top:10px;width:4px;height:4px;animation-delay:1.05s}
.osk-gradient{width:16px;height:16px;margin-left:4px;vertical-align:text-bottom;border-radius:9999px;background:conic-gradient(from 0deg,transparent,var(--osk-s));-webkit-mask:radial-gradient(farthest-side,transparent calc(100% - 2.5px),#000 calc(100% - 2px));mask:radial-gradient(farthest-side,transparent calc(100% - 2.5px),#000 calc(100% - 2px));animation:osk-spin .9s linear infinite}
@keyframes osk-fade{0%{opacity:1}100%{opacity:.15}}
@keyframes osk-tail{0%{opacity:1}50%,100%{opacity:.15}}
@keyframes osk-dna{0%,100%{transform:translateY(-5px) scale(.7)}50%{transform:translateY(5px) scale(1.2)}}
@keyframes osk-swing{from{transform:rotate(-35deg)}to{transform:rotate(35deg)}}
@keyframes osk-cla{0%,100%{transform:translateX(-6px)}25%,75%{transform:translateX(0)}}
@keyframes osk-clb{0%,25%,75%,100%{transform:translateX(0)}50%{transform:translateX(6px)}}
@keyframes osk-prog{0%{left:0;width:0}50%{left:0;width:75%}100%{left:100%;width:0}}
@keyframes osk-stripe{to{background-position:11.314px 0}}
@keyframes osk-hg{0%,40%{transform:rotate(0)}60%,100%{transform:rotate(180deg)}}
@keyframes osk-sq{0%,100%{opacity:.2;transform:scale(.7)}40%{opacity:1;transform:scale(1)}}
@keyframes osk-dia{0%,100%{transform:rotate(45deg);border-radius:1px}50%{transform:rotate(225deg) scale(.8);border-radius:50%}}
@keyframes osk-dw{0%,100%{transform:translateY(3px)}50%{transform:translateY(-3px)}}
@keyframes osk-bar{0%,100%{transform:scaleY(.25)}25%{transform:scaleY(1)}50%{transform:scaleY(.5)}75%{transform:scaleY(.8)}}
@keyframes osk-type{0%{width:0}75%,100%{width:100%}}
@keyframes osk-roll{0%,100%{transform:scaleX(.3)}50%{transform:scaleX(1)}}
@keyframes osk-chomp{from{clip-path:polygon(0 0,100% 0,100% 20%,50% 50%,100% 80%,100% 100%,0 100%)}to{clip-path:polygon(0 0,100% 0,100% 49%,50% 50%,100% 51%,100% 100%,0 100%)}}
@keyframes osk-dots{to{transform:translateX(-8px)}}
@keyframes osk-dash{to{stroke-dashoffset:-100}}
@keyframes osk-morph{0%,100%{border-radius:60% 40% 30% 70%/60% 30% 70% 40%;transform:rotate(0)}50%{border-radius:30% 60% 70% 40%/50% 60% 30% 60%;transform:rotate(180deg)}}
@keyframes osk-rise{0%{transform:translateY(0) scale(.6);opacity:0}20%{opacity:1}100%{transform:translateY(-13px) scale(1);opacity:0}}
@keyframes osk-bolt{0%,100%{opacity:.25;transform:scale(.85)}10%{opacity:1;transform:scale(1.1)}20%{opacity:.4}30%{opacity:1}60%{opacity:.25}}
@keyframes osk-tw{0%,100%{transform:scale(.3);opacity:.2}50%{transform:scale(1);opacity:1}}
"""


def _radial(cls, n, span_css, tf, container_extra=""):
    out = [
        f".osk-{cls}{{position:relative;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom;{container_extra}}}",
        f".osk-{cls}>span{{position:absolute;left:50%;top:50%;{span_css}}}",
    ]
    out += [f".osk-{cls}>span:nth-child({i + 1}){{{tf(i)}}}" for i in range(n)]
    return "\n".join(out)


def _delays(cls, values):
    return "\n".join(
        f".osk-{cls}>span:nth-child({i + 1}){{animation-delay:{v:g}s}}"
        for i, v in enumerate(values)
        if v
    )


_DOT = "border-radius:9999px;background:var(--osk-m)"
_ARC = [1, 0.65, 0.35]

EXTRA_CSS_STATIC = r"""
.osk-target{width:6px;height:6px;margin:0 7px;vertical-align:middle;border-radius:9999px;background:var(--osk-s);box-shadow:0 0 0 3px var(--osk-t),0 0 0 4.5px var(--osk-m);animation:osk-breathe 1.4s ease-in-out infinite}
.osk-squarejump{align-items:flex-end;gap:3px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-squarejump>span{display:block;width:4px;height:4px;border-radius:1px;background:var(--osk-m);animation:osk-jump 1s ease-in-out infinite}
.osk-seesaw{position:relative;width:20px;height:12px;margin-left:4px;vertical-align:text-bottom}
.osk-seesaw::before{content:"";position:absolute;left:0;right:0;top:50%;height:2px;margin-top:-1px;border-radius:1px;background:var(--osk-m);animation:osk-rock 1.2s ease-in-out infinite alternate}
.osk-seesaw::after{content:"";position:absolute;left:50%;bottom:0;margin-left:-3px;border-left:3px solid transparent;border-right:3px solid transparent;border-bottom:5px solid var(--osk-t)}
.osk-propeller{position:relative;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom;animation:osk-spin .9s linear infinite}
.osk-propeller::before,.osk-propeller::after{content:"";position:absolute;left:50%;top:1px;bottom:1px;width:2px;margin-left:-1px;border-radius:1px;background:var(--osk-m)}
.osk-propeller::after{transform:rotate(90deg);opacity:.5}
.osk-cog{box-sizing:border-box;width:14px;height:14px;margin:0 4px;vertical-align:text-bottom;border:3px dashed var(--osk-m);border-radius:9999px;animation:osk-spin 3s linear infinite}
.osk-dotted{box-sizing:border-box;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom;border:2px dotted var(--osk-m);border-radius:9999px;animation:osk-spin 2s linear infinite reverse}
.osk-marquee{width:28px;height:6px;margin-left:4px;vertical-align:middle;background:radial-gradient(circle,var(--osk-m) 1.6px,transparent 1.8px) 0 0/7px 6px repeat-x;animation:osk-mq .5s linear infinite}
.osk-flame{width:9px;height:13px;margin:0 4px;vertical-align:text-bottom;background:linear-gradient(to top,var(--osk-s),var(--osk-m));border-radius:50% 50% 50% 50%/70% 70% 30% 30%;transform-origin:50% 100%;animation:osk-flame .9s ease-in-out infinite alternate}
.osk-drop{position:relative;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom}
.osk-drop::before{content:"";position:absolute;left:50%;top:0;width:5px;height:6px;margin-left:-2.5px;background:var(--osk-m);border-radius:50% 50% 50% 50%/70% 70% 30% 30%;animation:osk-fall 1.2s ease-in infinite}
.osk-drop::after{content:"";position:absolute;left:50%;bottom:0;width:14px;height:5px;margin-left:-7px;box-sizing:border-box;border:1.5px solid var(--osk-m);border-radius:50%;opacity:0;animation:osk-splash 1.2s ease-out infinite}
.osk-rain{align-items:flex-start;gap:3px;height:14px;margin-left:4px;overflow:hidden;vertical-align:text-bottom}
.osk-rain>span{display:block;width:1.5px;height:5px;border-radius:1px;background:var(--osk-m);animation:osk-rain .9s linear infinite}
.osk-fill{position:relative;box-sizing:border-box;width:10px;height:14px;margin:0 5px;vertical-align:text-bottom;border:1.5px solid var(--osk-m);border-radius:2px;overflow:hidden}
.osk-fill::before{content:"";position:absolute;left:0;right:0;bottom:0;height:100%;background:var(--osk-m);transform-origin:50% 100%;animation:osk-fill 1.6s ease-in-out infinite}
.osk-battery{position:relative;box-sizing:border-box;width:20px;height:10px;margin:0 6px 0 4px;vertical-align:text-bottom;padding:1px;border:1.5px solid var(--osk-m);border-radius:2px}
.osk-battery::after{content:"";position:absolute;right:-4px;top:50%;width:2px;height:4px;margin-top:-2px;border-radius:0 1px 1px 0;background:var(--osk-m)}
.osk-battery>span{display:block;width:0;height:100%;background:var(--osk-m);animation:osk-batt 1.6s steps(4) infinite}
.osk-moon{box-sizing:border-box;width:12px;height:12px;margin:0 5px;vertical-align:middle;border:1px solid var(--osk-m);border-radius:9999px;background:var(--osk-t);animation:osk-moon 1.6s ease-in-out infinite alternate}
.osk-sun::after{content:"";position:absolute;left:50%;top:50%;width:6px;height:6px;margin:-3px 0 0 -3px;border-radius:9999px;background:var(--osk-s);animation:osk-breathe 1.6s ease-in-out infinite}
.osk-heart{width:14px;height:14px;margin:0 4px;vertical-align:text-bottom;color:var(--osk-m);animation:osk-heart 1.2s ease-in-out infinite}
.osk-star{width:14px;height:14px;margin:0 4px;vertical-align:text-bottom;color:var(--osk-m);animation:osk-star 1.6s ease-in-out infinite}
.osk-heart>svg,.osk-star>svg{display:block;width:100%;height:100%}
.osk-spectrum{align-items:center;gap:2px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-spectrum>span{display:block;width:2px;height:100%;border-radius:1px;background:var(--osk-m);animation:osk-wave .9s ease-in-out infinite}
.osk-bars3{align-items:center;gap:3px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-bars3>span{display:block;width:4px;height:100%;border-radius:2px;background:var(--osk-m);animation:osk-wave .8s ease-in-out infinite}
.osk-swap{position:relative;width:20px;height:7px;margin-left:4px;vertical-align:middle}
.osk-swap>span{position:absolute;top:0;width:7px;height:7px;border-radius:9999px}
.osk-swap>span:nth-child(1){left:0;background:var(--osk-m);animation:osk-sw 1s ease-in-out infinite}
.osk-swap>span:nth-child(2){left:13px;background:var(--osk-s);animation:osk-sw2 1s ease-in-out infinite}
.osk-satellite{position:relative;box-sizing:border-box;width:18px;height:18px;margin-left:4px;vertical-align:text-bottom;border:1px solid var(--osk-t);border-radius:9999px}
.osk-satellite::before{content:"";position:absolute;left:50%;top:50%;width:5px;height:5px;margin:-2.5px 0 0 -2.5px;border-radius:9999px;background:var(--osk-s)}
.osk-satellite>span{position:absolute;top:0;right:0;bottom:0;left:0;animation:osk-spin 1.4s linear infinite}
.osk-satellite>span::after{content:"";position:absolute;left:50%;top:-3px;width:4px;height:4px;margin-left:-2px;border-radius:9999px;background:var(--osk-m)}
.osk-atom{position:relative;width:16px;height:16px;margin-left:4px;vertical-align:text-bottom;animation:osk-spin 2.4s linear infinite}
.osk-atom>span{position:absolute;left:0;top:50%;box-sizing:border-box;width:16px;height:6px;margin-top:-3px;border:1px solid var(--osk-m);border-radius:50%}
.osk-atom>span:nth-child(2){transform:rotate(60deg)}
.osk-atom>span:nth-child(3){transform:rotate(120deg)}
.osk-atom::after{content:"";position:absolute;left:50%;top:50%;width:3px;height:3px;margin:-1.5px 0 0 -1.5px;border-radius:9999px;background:var(--osk-s)}
.osk-binary{align-items:center;gap:1px;height:14px;margin-left:4px;vertical-align:text-bottom;font:600 10px/1 ui-monospace,SFMono-Regular,Menlo,monospace;color:var(--osk-m)}
.osk-binary>span{display:block;animation:osk-seq 1.2s ease-in-out infinite}
.osk-binary>span::before{content:"0"}
.osk-binary>span:nth-child(1)::before,.osk-binary>span:nth-child(3)::before,.osk-binary>span:nth-child(4)::before{content:"1"}
.osk-segments{align-items:center;gap:2px;height:6px;margin-left:4px;vertical-align:middle}
.osk-segments>span{display:block;width:5px;height:4px;border-radius:1px;background:var(--osk-m);animation:osk-seq 1.2s ease-in-out infinite}
.osk-zigzag{width:24px;height:12px;margin-left:4px;vertical-align:text-bottom;color:var(--osk-m)}
.osk-zigzag>svg{display:block;width:100%;height:100%;overflow:visible}
.osk-zigzag path{fill:none;stroke:currentColor;stroke-width:1.5;stroke-linecap:round;stroke-linejoin:round;stroke-dasharray:50;stroke-dashoffset:50;animation:osk-ecg 1.6s linear infinite}
.osk-squaretrace,.osk-hexagon{width:16px;height:16px;margin:0 3px;vertical-align:text-bottom;color:var(--osk-m)}
.osk-squaretrace>svg,.osk-hexagon>svg{display:block;width:100%;height:100%;overflow:visible}
.osk-squaretrace path,.osk-hexagon path{fill:none;stroke:currentColor;stroke-width:1.6;stroke-linecap:round;stroke-linejoin:round}
.osk-squaretrace .t,.osk-hexagon .t{opacity:.25}
.osk-squaretrace .a,.osk-hexagon .a{stroke-dasharray:30 70;animation:osk-dash 1.4s linear infinite}
.osk-pingpong{position:relative;width:30px;height:10px;margin-left:4px;vertical-align:text-bottom}
.osk-pingpong::before,.osk-pingpong::after{content:"";position:absolute;top:0;bottom:0;width:2px;border-radius:1px;background:var(--osk-t)}
.osk-pingpong::before{left:0}
.osk-pingpong::after{right:0}
.osk-pingpong>span{position:absolute;left:4px;top:50%;width:4px;height:4px;margin-top:-2px;border-radius:9999px;background:var(--osk-s);animation:osk-pp .7s linear infinite alternate}
.osk-traffic{flex-direction:column;align-items:center;justify-content:center;gap:1px;width:6px;height:16px;margin:0 6px;vertical-align:text-bottom}
.osk-traffic>span{display:block;width:4px;height:4px;border-radius:9999px;background:var(--osk-m);animation:osk-seq 1.2s ease-in-out infinite}
.osk-search{position:relative;box-sizing:border-box;width:10px;height:10px;margin:0 7px 0 3px;vertical-align:middle;border:1.5px solid var(--osk-m);border-radius:9999px;animation:osk-wob 1.2s ease-in-out infinite alternate}
.osk-search::after{content:"";position:absolute;left:6.5px;top:6.5px;width:5px;height:1.5px;border-radius:1px;background:var(--osk-m);transform-origin:0 50%;transform:rotate(45deg)}
.osk-matrix{grid-template-columns:repeat(4,3px);gap:2px;margin-left:4px;vertical-align:text-bottom}
.osk-matrix>span{display:block;width:3px;height:3px;border-radius:1px;background:var(--osk-m);animation:osk-grid 1.2s ease-in-out infinite}
.osk-rotsq{width:9px;height:9px;margin:0 5px;vertical-align:middle;border-radius:2px;background:var(--osk-m);animation:osk-rsq 1.4s ease-in-out infinite}
.osk-quad{grid-template-columns:repeat(2,5px);gap:2px;margin:0 3px 0 4px;vertical-align:text-bottom;animation:osk-quad 1.6s ease-in-out infinite}
.osk-quad>span{display:block;width:5px;height:5px;border-radius:1px;background:var(--osk-m)}
.osk-quad>span:nth-child(2),.osk-quad>span:nth-child(3){background:var(--osk-s)}
.osk-knight{position:relative;width:35px;height:6px;margin-left:4px;vertical-align:middle;background:radial-gradient(circle,var(--osk-t) 2px,transparent 2.2px) 0 0/7px 6px repeat-x}
.osk-knight::after{content:"";position:absolute;top:0;right:0;bottom:0;left:0;background:radial-gradient(circle,var(--osk-s) 2px,transparent 2.2px) 0 0/7px 6px repeat-x;-webkit-mask:linear-gradient(90deg,transparent,#000,transparent) 0 0/16px 100% no-repeat;mask:linear-gradient(90deg,transparent,#000,transparent) 0 0/16px 100% no-repeat;animation:osk-kr 1s ease-in-out infinite alternate}
.osk-thought{align-items:flex-end;gap:3px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-thought>span{display:block;border-radius:9999px;background:var(--osk-m);animation:osk-seq 1.2s ease-in-out infinite}
.osk-thought>span:nth-child(1){width:2px;height:2px}
.osk-thought>span:nth-child(2){width:3.5px;height:3.5px}
.osk-thought>span:nth-child(3){width:6px;height:6px}
.osk-diamonds{align-items:center;gap:4px;height:14px;margin-left:4px;vertical-align:text-bottom}
.osk-diamonds>span{display:block;width:4px;height:4px;background:var(--osk-m);transform:rotate(45deg);animation:osk-dmd 1.2s ease-in-out infinite}
.osk-snow{align-items:flex-start;gap:4px;height:14px;margin-left:4px;overflow:hidden;vertical-align:text-bottom}
.osk-snow>span{display:block;width:3px;height:3px;border-radius:9999px;background:var(--osk-m);animation:osk-snow 1.8s linear infinite}
@keyframes osk-jump{0%,60%,100%{transform:translateY(0) rotate(0)}30%{transform:translateY(-8px) rotate(180deg)}}
@keyframes osk-rock{from{transform:rotate(-18deg)}to{transform:rotate(18deg)}}
@keyframes osk-mq{to{background-position:7px 0}}
@keyframes osk-flame{0%{transform:scale(1,.85) skewX(-4deg);opacity:.7}50%{transform:scale(.9,1.05) skewX(3deg);opacity:1}100%{transform:scale(1.05,.9) skewX(-2deg);opacity:.85}}
@keyframes osk-fall{0%{transform:translateY(0);opacity:1}55%{transform:translateY(8px);opacity:1}56%,100%{transform:translateY(8px);opacity:0}}
@keyframes osk-splash{0%,55%{transform:scale(.1);opacity:0}60%{opacity:1}100%{transform:scale(1);opacity:0}}
@keyframes osk-rain{0%{transform:translateY(-6px);opacity:0}30%{opacity:1}100%{transform:translateY(14px);opacity:0}}
@keyframes osk-fill{0%{transform:scaleY(0)}70%,100%{transform:scaleY(1)}}
@keyframes osk-batt{0%{width:0}100%{width:100%}}
@keyframes osk-moon{from{box-shadow:inset 0 0 0 0 var(--osk-m)}to{box-shadow:inset 13px 0 0 0 var(--osk-m)}}
@keyframes osk-heart{0%,60%,100%{transform:scale(1)}15%{transform:scale(1.25)}30%{transform:scale(1)}45%{transform:scale(1.18)}}
@keyframes osk-star{0%,100%{transform:rotate(0) scale(.8)}50%{transform:rotate(72deg) scale(1.1)}}
@keyframes osk-sw{0%,100%{transform:translateX(0)}50%{transform:translateX(13px) scale(.8)}}
@keyframes osk-sw2{0%,100%{transform:translateX(0)}50%{transform:translateX(-13px) scale(.8)}}
@keyframes osk-pp{from{transform:translateX(0)}to{transform:translateX(20px)}}
@keyframes osk-wob{from{transform:translate(-2px,-1px) rotate(-12deg)}to{transform:translate(2px,1px) rotate(12deg)}}
@keyframes osk-rsq{0%{transform:rotate(0) scale(1)}50%{transform:rotate(90deg) scale(.6)}100%{transform:rotate(180deg) scale(1)}}
@keyframes osk-quad{0%,15%{transform:rotate(0)}50%{transform:rotate(180deg) scale(.85)}85%,100%{transform:rotate(360deg)}}
@keyframes osk-kr{from{-webkit-mask-position:0 0;mask-position:0 0}to{-webkit-mask-position:100% 0;mask-position:100% 0}}
@keyframes osk-dmd{0%,100%{opacity:.25;transform:rotate(45deg) scale(.7)}40%{opacity:1;transform:rotate(45deg) scale(1.15)}}
@keyframes osk-snow{0%{transform:translate(0,-4px);opacity:0}20%{opacity:1}50%{transform:translate(2px,5px)}80%{opacity:1}100%{transform:translate(-1px,14px);opacity:0}}
"""

EXTRA_CSS = "\n".join(
    [
        EXTRA_CSS_STATIC.strip(),
        _radial(
            "spiral",
            8,
            f"width:2.5px;height:2.5px;margin:-1.25px 0 0 -1.25px;{_DOT};animation:osk-fade 1s linear infinite",
            lambda i: f"transform:rotate({i * 47}deg) translateY(-{1.2 + i * .85:.2f}px);animation-delay:-{i / 8:.3f}s",
        ),
        _radial(
            "ios",
            12,
            f"width:1.5px;height:4px;margin:-2px 0 0 -.75px;border-radius:1px;background:var(--osk-m);animation:osk-fade 1s linear infinite",
            lambda i: f"transform:rotate({i * 30}deg) translateY(-5px);animation-delay:-{i / 12:.3f}s",
        ),
        _radial(
            "comet",
            6,
            f"width:4px;height:4px;margin:-2px 0 0 -2px;{_DOT}",
            lambda i: f"transform:rotate(-{i * 24}deg) translateY(-6px) scale({1 - i * .13:.2f});opacity:{1 - i * .16:.2f}",
            "animation:osk-spin .9s linear infinite;",
        ),
        _radial(
            "sun",
            8,
            "width:1.5px;height:3.5px;margin:-1.75px 0 0 -.75px;border-radius:1px;background:var(--osk-m)",
            lambda i: f"transform:rotate({i * 45}deg) translateY(-7px)",
            "animation:osk-spin 6s linear infinite;",
        ),
        _radial(
            "trio",
            3,
            f"width:4px;height:4px;margin:-2px 0 0 -2px;{_DOT}",
            lambda i: f"transform:rotate({i * 120}deg) translateY(-6px);opacity:{_ARC[i]}",
            "animation:osk-spin .9s linear infinite;",
        ),
        _delays("squarejump", [0, 0.15, 0.3]),
        _delays("rain", [0, -0.3, -0.6, -0.15, -0.45]),
        _delays("spectrum", [0, -0.5, -0.2, -0.7, -0.35, -0.1, -0.6]),
        _delays("bars3", [0, -0.15, -0.3]),
        _delays("binary", [0, 0.15, 0.3, 0.45, 0.6]),
        _delays("segments", [0, 0.12, 0.24, 0.36, 0.48, 0.6]),
        _delays("traffic", [0, 0.2, 0.4]),
        _delays("matrix", [-((i * 7) % 11) * 0.1 for i in range(16)]),
        _delays("thought", [0, 0.2, 0.4]),
        _delays("diamonds", [0, 0.15, 0.3]),
        _delays("snow", [0, -0.45, -0.9, -1.35]),
    ]
)

MORE_CSS = "\n".join(
    [
        MORE_CSS_STATIC.strip(),
        _ring_css("chase", 8, 6, 3, 0.9, "osk-tail"),
        _ring_css("dotring", 12, 6.5, 2.2, 1.2, "osk-fade"),
        EXTRA_CSS,
        V2_CSS.strip(),
    ]
)


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


# ---------------------------------------------------------------------------
# 2.0.0: Thinking Words. Replaces the "Thinking..." label with rotating words.
# ---------------------------------------------------------------------------
TW_PACKS = {
    "claude": [
        "Thinking this through",
        "Going over the details",
        "Pondering",
        "Trying harder",
        "Mulling it over",
        "Connecting the dots",
        "Chewing on that",
        "Weighing options",
        "Digging deeper",
        "Untangling this",
        "Cooking up an answer",
        "Working it out",
        "Reading between the lines",
        "Considering angles",
        "Double-checking",
        "Sharpening the answer",
        "Almost there",
        "Gathering my thoughts",
        "Turning it over",
        "Following the thread",
        "Getting to the bottom of it",
        "Looking at the big picture",
        "Breaking it down",
        "Piecing it together",
        "Taking a closer look",
        "Exploring possibilities",
        "Checking my reasoning",
        "Putting it all together",
        "Examining the evidence",
        "Sorting through ideas",
        "Connecting the pieces",
        "Thinking it over",
        "Giving this some thought",
        "Reflecting on this",
        "Analyzing the question",
        "Looking at it from every side",
        "Trying another angle",
        "Working through the logic",
        "Making sense of it",
        "Searching my memory",
        "Narrowing it down",
        "Drawing conclusions",
        "Crafting a response",
        "Organizing my thoughts",
        "Finding the right words",
        "Refining the answer",
        "Polishing things up",
        "Checking the facts",
        "Considering the options",
        "Balancing the details",
        "Looking deeper",
        "Thinking carefully",
        "Giving it my full attention",
        "Doing the heavy lifting",
        "Running through scenarios",
        "Testing a few ideas",
        "Zooming in",
        "Zooming out",
        "Rethinking this",
        "Second-guessing myself",
        "Making a plan",
        "Mapping it out",
        "Laying it all out",
        "Digesting this",
        "Absorbing the details",
        "Reading it again",
        "Taking it step by step",
        "Thinking out loud",
        "Thinking really hard",
        "Brainstorming",
        "Deliberating",
        "Contemplating",
        "Ruminating",
        "Cogitating",
        "Noodling on it",
        "Wrestling with this",
        "Doing some mental math",
        "Building the answer",
        "Assembling my thoughts",
        "Looking for the best approach",
        "Choosing my words",
        "Picking the best answer",
        "Putting in the effort",
        "Thinking deeply",
        "Staying with it",
        "Going the extra mile",
        "Making sure it is right",
        "Cross-checking",
        "Reviewing my notes",
        "Sketching it out",
        "Finding the pattern",
        "Spotting the catch",
        "Tracing the logic",
        "Chasing an idea",
        "Catching a thought",
        "Letting ideas simmer",
        "Warming up",
        "Getting my bearings",
        "Gaining clarity",
        "Seeing the whole picture",
        "Getting the details right",
        "Tying up loose ends",
        "Weaving it together",
        "Pulling it together",
        "Fine-tuning",
        "One moment",
        "Hang tight",
        "Bear with me",
        "Just a sec",
        "Working on it",
        "Give me a moment",
        "Deep in thought",
        "Lost in thought",
        "Wheels turning",
        "Gears turning",
        "Brain whirring",
        "Neurons firing",
        "Lightbulb loading",
        "Coming up with something good",
    ],
    "dev": [
        "Fixing bugs",
        "Compiling thoughts",
        "Refactoring ideas",
        "Reading the docs",
        "Rubber-ducking",
        "Running tests",
        "Resolving merge conflicts",
        "Grepping my memory",
        "Optimizing",
        "Checking edge cases",
        "Linting my thoughts",
        "Clearing the cache",
        "Reviewing the diff",
        "Writing a unit test",
        "Stack Overflow-ing",
        "Deploying an answer",
        "Squashing commits",
        "Profiling the problem",
        "Blaming the cache",
        "Shipping it",
    ],
    "playful": [
        "Consulting the oracle",
        "Asking the hamsters",
        "Herding thoughts",
        "Warming up brain cells",
        "Summoning wisdom",
        "Shuffling neurons",
        "Polishing the crystal ball",
        "Feeding the gremlins",
        "Stretching synapses",
        "Brewing coffee for neurons",
        "Doing mental gymnastics",
        "Waking the wizards",
        "Spinning the wheel",
        "Poking the idea box",
        "Tickling the hamster wheel",
        "Cracking knuckles",
    ],
    "science": [
        "Running the numbers",
        "Formulating hypotheses",
        "Calibrating",
        "Simulating outcomes",
        "Cross-referencing",
        "Testing theories",
        "Measuring twice",
        "Crunching data",
        "Peer reviewing myself",
        "Isolating variables",
        "Balancing the equation",
        "Observing patterns",
    ],
    "hacker": [
        "Initializing neural net",
        "Decrypting the question",
        "Parsing intent",
        "Allocating brain",
        "Handshaking with logic",
        "Booting reasoning core",
        "Tracing the stack",
        "Injecting insight",
        "Compiling answer",
        "Bypassing the obvious",
        "Sudo thinking",
        "Opening a root shell",
    ],
    "zen": [
        "Breathing",
        "Finding clarity",
        "Letting it settle",
        "Taking a moment",
        "Focusing",
        "Seeking balance",
        "Quieting the noise",
        "Listening closely",
    ],
    "minimal": ["Thinking", "Reasoning", "Working", "Processing", "Considering"],
}

TW_PACK_NAMES = {
    "default": "Default",
    "off": "Original \u201cThinking\u2026\u201d",
    "claude": "Claude-style",
    "dev": "Dev & Debug",
    "playful": "Playful",
    "science": "Science Lab",
    "hacker": "Hacker Terminal",
    "zen": "Zen",
    "minimal": "Minimal",
    "mix": "Random Mix (all packs)",
    "custom": "Custom (admin list)",
}
TW_GLYPHS = {
    "default": "Aa",
    "off": "\u2026",
    "claude": "\u273a",
    "dev": "</>",
    "playful": "\u2728",
    "science": "\u2697",
    "hacker": ">_",
    "zen": "\u262f",
    "minimal": "\u00b7",
    "mix": "\u221e",
    "custom": "\u270e",
}
TW_FX_NAMES = {
    "default": "Default",
    "random": "Random (changes every phrase)",
    "shimmer": "Shimmer Sweep",
    "gradient": "Gradient Flow",
    "rainbow": "Rainbow",
    "neon": "Neon Flicker",
    "glow": "Soft Glow",
    "fade": "Fade Rise",
    "slide": "Slot Slide",
    "typewriter": "Typewriter",
    "scramble": "Decode / Scramble",
    "blur": "Blur In",
    "wave": "Letter Wave",
    "bounce": "Letter Drop",
    "cascade": "Letter Cascade",
    "glitch": "Glitch",
    "pop": "Pop",
    "flip": "Flip In",
    "reveal": "Wipe Reveal",
    "pulse": "Soft Pulse",
    "plain": "Plain",
}
TW_FX_POOL = [k for k in TW_FX_NAMES if k not in ("default", "random", "plain")]
TW_FONTS = {
    "mono": "font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,'Liberation Mono',monospace;font-size:.92em",
    "serif": "font-family:Georgia,'Times New Roman',serif",
    "italic": "font-family:'Palatino Linotype',Palatino,'Book Antiqua',Georgia,serif;font-style:italic",
    "rounded": "font-family:ui-rounded,'SF Pro Rounded',Nunito,Quicksand,'Varela Round',system-ui,sans-serif;font-weight:600",
    "hand": "font-family:'Segoe Print','Bradley Hand','Chalkboard SE','Comic Sans MS','Comic Neue',cursive;font-size:1.05em",
    "caps": "text-transform:uppercase;letter-spacing:.14em;font-size:.78em;font-weight:600",
    "small": "font-variant:small-caps;letter-spacing:.08em;font-size:1.05em",
}
TW_FONT_NAMES = {
    "default": "Default",
    "random": "Random (changes every phrase)",
    "theme": "Match Theme",
    "mono": "Monospace",
    "serif": "Serif",
    "italic": "Elegant Italic",
    "rounded": "Rounded",
    "hand": "Handwritten",
    "caps": "Wide Caps",
    "small": "Small Caps",
}
TW_FONT_POOL = ["theme"] + list(TW_FONTS)
TW_COLORS = {
    "orange": ("#d97757", "#ffb59a", "#b4532f"),
    "blue": ("#3b82f6", "#93c5fd", "#1d4ed8"),
    "purple": ("#8b5cf6", "#c4b5fd", "#6d28d9"),
    "green": ("#10b981", "#6ee7b7", "#047857"),
    "pink": ("#ec4899", "#f9a8d4", "#be185d"),
    "gold": ("#f59e0b", "#fde68a", "#b45309"),
    "cyan": ("#06b6d4", "#67e8f9", "#0e7490"),
    "aurora": ("#34d399", "#60a5fa", "#a78bfa"),
    "sunset": ("#f97316", "#ec4899", "#8b5cf6"),
}
TW_COLOR_NAMES = {
    "default": "Default",
    "random": "Random (changes every phrase)",
    "theme": "Match Theme",
    "orange": "Claude Orange",
    "blue": "Blue",
    "purple": "Purple",
    "green": "Green",
    "pink": "Pink",
    "gold": "Gold",
    "cyan": "Cyan",
    "aurora": "Aurora",
    "sunset": "Sunset",
}
TW_RHYTHM_NAMES = {
    "default": "Default",
    "steady": "Steady",
    "natural": "Natural (varies +/-40%)",
}
TW_TIMER_NAMES = {"default": "Default", "off": "Off", "on": "Show seconds"}

TW_PACK_LITERAL = Literal[tuple(k for k in TW_PACK_NAMES if k != "default")]
TW_FX_LITERAL = Literal[tuple(k for k in TW_FX_NAMES if k != "default")]
TW_FONT_LITERAL = Literal[tuple(k for k in TW_FONT_NAMES if k != "default")]
TW_COLOR_LITERAL = Literal[tuple(k for k in TW_COLOR_NAMES if k != "default")]
TW_RHYTHM_LITERAL = Literal[tuple(k for k in TW_RHYTHM_NAMES if k != "default")]

TW_CSS_BASE = r"""
:root{--tw-a:#6b7280;--tw-b:#111827;--tw-c:#9ca3af;--osk-tw-dur:2400ms}
html.dark{--tw-a:#8e8e93;--tw-b:#ffffff;--tw-c:#636366}
.osk-orig{display:none!important}
.osk-nosh{background:none!important;animation:none!important}
.osk-sr{position:absolute;width:1px;height:1px;margin:-1px;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap}
.osk-tw{display:inline-flex;align-items:baseline;gap:0;vertical-align:baseline;max-width:100%}
.osk-tw[data-clip="1"]{clip-path:inset(0)}
.osk-tw-w,.osk-pv-s{display:inline-block;position:relative;white-space:nowrap;color:var(--tw-a);-webkit-text-fill-color:var(--tw-a);font-weight:500;line-height:1.3}
.osk-pv.osk-pv-t{width:88px}
.osk-pv.osk-pv-t>span{display:inline-block}
.osk-pv-s{--osk-tw-dur:2600ms;font-size:.8rem;margin-left:4px}
.osk-pv-t:has(.osk-pv-s[data-fx="slide"]){clip-path:inset(0)}
.osk-l{display:inline-block}
.osk-tw-d{display:none}
html[data-osk-tw-dots] .osk-tw-d{display:inline-flex;margin-left:.06em;letter-spacing:0;line-height:1.3}
.osk-tw-d i{font-style:normal;color:var(--tw-a);-webkit-text-fill-color:var(--tw-a);animation:osk-tw-dot 1.2s infinite}
.osk-tw-d i::before{content:'.'}
.osk-tw-d i:nth-child(2){animation-delay:.2s}
.osk-tw-d i:nth-child(3){animation-delay:.4s}
.osk-tw-t{display:none;margin-left:.3em;opacity:.55;font-size:.9em;font-variant-numeric:tabular-nums;-webkit-text-fill-color:currentColor}
html[data-osk-tw-timer="on"] .osk-tw-t{display:inline}
.osk-tw-t::before{content:'\00b7\00a0'}
.osk-heading{margin:.85rem 0 .5rem;font-size:.6875rem;font-weight:600;letter-spacing:.06em;text-transform:uppercase;color:var(--color-gray-400,#9ca3af)}
.osk-range-row{display:flex;align-items:center;gap:8px}
.osk-range{width:128px;height:4px;cursor:pointer;accent-color:#d97757}
.osk-range-v{min-width:2.4rem;text-align:right;font-size:.75rem;font-variant-numeric:tabular-nums;color:var(--color-gray-500,#6b7280)}
.osk-range-r{padding:0;border:0;background:none;font-size:.6875rem;text-decoration:underline;cursor:pointer;color:var(--color-gray-400,#9ca3af)}
.osk-range-r[hidden]{display:none}
.osk-pv-s[data-color="aurora"],.osk-pv-s[data-color="sunset"],.osk-pv-s[data-color="random"]{background:linear-gradient(90deg,var(--tw-a),var(--tw-b),var(--tw-c));-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}
.osk-pv-s[data-color="random"]{--tw-a:#f87171;--tw-b:#fbbf24;--tw-c:#60a5fa}
@keyframes osk-tw-in{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:none}}
@keyframes osk-tw-out{from{opacity:1;transform:none}to{opacity:0;transform:translateY(-5px)}}
@keyframes osk-tw-shim{from{background-position:200% 0}to{background-position:-100% 0}}
@keyframes osk-tw-flow{from{background-position:0 0}to{background-position:300% 0}}
@keyframes osk-tw-slidein{from{transform:translateY(115%);opacity:0}to{transform:none;opacity:1}}
@keyframes osk-tw-slideout{from{transform:none;opacity:1}to{transform:translateY(-115%);opacity:0}}
@keyframes osk-tw-type{from{clip-path:inset(0 100% 0 0)}to{clip-path:inset(0 0 0 0)}}
@keyframes osk-tw-caret{50%{background:transparent}}
@keyframes osk-tw-blurin{from{filter:blur(10px);opacity:0}to{filter:none;opacity:1}}
@keyframes osk-tw-blurout{from{filter:none;opacity:1}to{filter:blur(10px);opacity:0}}
@keyframes osk-tw-glow{from{text-shadow:0 0 2px var(--tw-a),0 0 6px var(--tw-a)}to{text-shadow:0 0 7px var(--tw-b),0 0 18px var(--tw-a)}}
@keyframes osk-tw-flick{0%,19%,21%,23%,25%,54%,56%,100%{opacity:1}20%,24%,55%{opacity:.3}}
@keyframes osk-tw-bob{0%,60%,100%{transform:none}30%{transform:translateY(-3px)}}
@keyframes osk-tw-drop{from{transform:translateY(-1.1em);opacity:0}to{transform:none;opacity:1}}
@keyframes osk-tw-gl{0%,88%,100%{opacity:0;transform:none}91%{opacity:.9;transform:translate(-3px,1px)}95%{opacity:.9;transform:translate(3px,-1px)}}
@keyframes osk-tw-glm{0%,89%,100%{transform:none}91%{transform:translate(-2px,0) skewX(10deg)}94%{transform:translate(2px,1px) skewX(-8deg)}96%{transform:none}}
@keyframes osk-tw-pop{from{transform:scale(.5);opacity:0}to{transform:none;opacity:1}}
@keyframes osk-tw-flip{from{transform:perspective(220px) rotateX(-90deg);opacity:0}to{transform:perspective(220px) rotateX(0);opacity:1}}
@keyframes osk-tw-reveal{from{-webkit-mask-position:100% 0;mask-position:100% 0}to{-webkit-mask-position:0 0;mask-position:0 0}}
@keyframes osk-tw-pulse{0%,100%{opacity:.5}50%{opacity:1}}
@keyframes osk-tw-dot{0%,80%,100%{opacity:.2}40%{opacity:1}}
"""


def _tw_css() -> str:
    out = [TW_CSS_BASE.strip()]
    OUTD = "max(0s,calc(var(--osk-tw-dur) - .45s))"
    clip = "-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent"

    def fx(k, decl, pseudo="", child=""):
        out.append(
            f'.osk-tw-w[data-fx="{k}"]{child}{pseudo},.osk-pv-s[data-fx="{k}"]{child}{pseudo}{{{decl}}}'
        )

    fx(
        "shimmer",
        "background:linear-gradient(100deg,var(--tw-a) 38%,var(--tw-b) 50%,var(--tw-a) 62%) 200% 0/250% 100%;"
        + clip
        + ";animation:osk-tw-in .4s ease both,osk-tw-shim 1.8s linear infinite",
    )
    fx(
        "gradient",
        "background:linear-gradient(90deg,var(--tw-a),var(--tw-b),var(--tw-c),var(--tw-a)) 0 0/300% 100%;"
        + clip
        + ";animation:osk-tw-in .4s ease both,osk-tw-flow 3s linear infinite",
    )
    fx(
        "rainbow",
        "background:linear-gradient(90deg,#f87171,#fbbf24,#34d399,#60a5fa,#a78bfa,#f87171) 0 0/300% 100%;"
        + clip
        + ";animation:osk-tw-in .4s ease both,osk-tw-flow 3.5s linear infinite",
    )
    fx(
        "neon",
        "color:var(--tw-b);-webkit-text-fill-color:var(--tw-b);text-shadow:0 0 4px var(--tw-a),0 0 12px var(--tw-a),0 0 26px var(--tw-a);animation:osk-tw-in .3s ease both,osk-tw-flick 3s linear infinite",
    )
    fx(
        "glow",
        "color:var(--tw-b);-webkit-text-fill-color:var(--tw-b);animation:osk-tw-in .4s ease both,osk-tw-glow 1.6s ease-in-out infinite alternate",
    )
    fx(
        "fade",
        f"animation:osk-tw-in .4s ease both,osk-tw-out .4s ease-in {OUTD} forwards",
    )
    fx(
        "slide",
        f"animation:osk-tw-slidein .45s cubic-bezier(.2,.8,.2,1) both,osk-tw-slideout .4s ease-in {OUTD} forwards",
    )
    fx(
        "typewriter",
        "animation:osk-tw-type min(1s,calc(var(--osk-tw-dur)*.45)) steps(var(--n,12),end) both",
    )
    fx(
        "typewriter",
        "content:'';display:inline-block;width:.09em;height:1em;margin-left:2px;vertical-align:-.14em;background:var(--tw-a);-webkit-text-fill-color:initial;animation:osk-tw-caret .8s step-end infinite",
        "::after",
    )
    fx(
        "scramble",
        "font-variant-numeric:tabular-nums;letter-spacing:.02em;animation:osk-tw-in .2s ease both",
    )
    fx(
        "blur",
        f"animation:osk-tw-blurin .6s ease both,osk-tw-blurout .5s ease-in {OUTD} forwards",
    )
    fx("wave", "animation:osk-tw-in .4s ease both")
    fx(
        "wave",
        "animation:osk-tw-bob 1.4s ease-in-out infinite;animation-delay:calc(var(--i,0)*70ms)",
        child=" .osk-l",
    )
    fx(
        "bounce",
        "animation:osk-tw-drop .6s cubic-bezier(.3,1.6,.5,1) both;animation-delay:calc(var(--i,0)*45ms)",
        child=" .osk-l",
    )
    fx(
        "cascade",
        "animation:osk-tw-in .5s ease both;animation-delay:calc(var(--i,0)*40ms)",
        child=" .osk-l",
    )
    fx("glitch", "animation:osk-tw-in .3s ease both,osk-tw-glm 2.4s steps(1) infinite")
    g = "content:attr(data-text);position:absolute;left:0;top:0;opacity:0;pointer-events:none;-webkit-text-fill-color:currentColor"
    fx("glitch", g + ";color:#22d3ee;animation:osk-tw-gl 2.4s infinite", "::before")
    fx(
        "glitch",
        g + ";color:#f472b6;animation:osk-tw-gl 2.4s .08s infinite reverse",
        "::after",
    )
    fx("pop", "animation:osk-tw-pop .5s cubic-bezier(.3,1.7,.5,1) both")
    fx("flip", "transform-origin:50% 60%;animation:osk-tw-flip .6s ease both")
    fx(
        "reveal",
        "-webkit-mask-image:linear-gradient(90deg,#000 45%,transparent 65%);mask-image:linear-gradient(90deg,#000 45%,transparent 65%);-webkit-mask-size:300% 100%;mask-size:300% 100%;animation:osk-tw-reveal 1s ease both",
    )
    fx(
        "pulse",
        "animation:osk-tw-in .4s ease both,osk-tw-pulse 1.6s ease-in-out .4s infinite",
    )

    for k, decl in TW_FONTS.items():
        out.append(f'.osk-tw-w[data-font="{k}"],.osk-pv-s[data-font="{k}"]{{{decl}}}')
    for k, (a, b, c) in TW_COLORS.items():
        out.append(
            f'html[data-osk-tw-color="{k}"],.osk-pv-s[data-color="{k}"]{{--tw-a:{a};--tw-b:{b};--tw-c:{c}}}'
        )
    out.append(
        "@media (prefers-reduced-motion:reduce){.osk-tw-w,.osk-tw-w::before,.osk-tw-w::after,.osk-l,.osk-tw-d i,.osk-pv-s{"
        "animation:none!important;background:none!important;clip-path:none!important;filter:none!important;text-shadow:none!important;"
        "-webkit-mask-image:none!important;mask-image:none!important;-webkit-text-fill-color:var(--tw-a)!important}"
        ".osk-tw-w::before,.osk-tw-w::after{display:none}}"
    )
    return "\n".join(out)


TW_CSS = _tw_css()

OSK_CSS = "\n".join(
    [
        OSK_CSS_BASE.strip(),
        OSK_CSS_NEW.strip(),
        MORE_CSS,
        _build_generated_css(),
        TW_CSS,
    ]
)

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
    '<span class="osk-ball"><span></span></span>' +
    __MORE_MARKUP__;
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

    var POOL = ORDER.filter(function (k) { return k !== 'default' && k !== 'none' && k !== 'random'; });

    function roll() {
      var cur = ROOT.getAttribute(spec.attr);
      var pick = cur;
      for (var tries = 0; tries < 8 && (pick === cur || !pick); tries++) pick = POOL[Math.floor(Math.random() * POOL.length)];
      ROOT.setAttribute(spec.attr, pick);
    }

    function apply() {
      var value = effective();
      if (value === 'random' && spec.roll) roll();
      else if (value === 'default') ROOT.removeAttribute(spec.attr);
      else ROOT.setAttribute(spec.attr, value);
    }

    function label(value) {
      if (value !== 'default' || spec.fallback === 'default') return NAMES[value];
      return NAMES.default + ' (' + (NAMES[spec.fallback] || spec.fallback) + ')';
    }

    function preview(value) {
      var shown = value === 'default' ? spec.fallback : value;
      if (shown === 'default' && spec.defaultPreview) shown = spec.defaultPreview;
      if (spec.preview) return spec.preview(shown, value);
      var el = document.createElement('span');
      el.className = 'osk-pv';
      el.setAttribute('aria-hidden', 'true');
      if (shown === 'random') {
        el.textContent = '\uD83C\uDFB2';
        el.style.paddingLeft = '4px';
      } else if (shown !== 'default' && shown !== 'none') {
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
      if (spec.heading) {
        var head = document.createElement('div');
        head.className = 'osk-heading';
        head.textContent = spec.heading;
        block.appendChild(head);
      }
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
    self.reroll = function () { if (spec.roll && effective() === 'random') roll(); };
    self.closeMenu = closeMenu;
    self.ensureRow = ensureRow;
    self.check = check;
    return self;
  }

  var message = makePicker({
    id: 'osk-setting',
    key: 'owui-message-loading-indicator',
    attr: 'data-osk',
    roll: true,
    names: CFG.names,
    fallback: CFG.fallback,
    defaultPreview: 'cursor',
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
    roll: true,
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

  // =====================================================================
  // v2: Thinking Words. Swaps Open WebUI's "Thinking..." text for rotating
  // words. The original text node is MOVED (not copied) into a hidden wrapper,
  // so Svelte keeps updating it and everything is restored when thinking ends.
  // =====================================================================
  var TW = (function () {
    var T = CFG.tw;
    var pickers = [];
    var timer = null;
    var pvTimer = null;
    var KEY_MS = 'owui-thinking-words-interval';
    var LETTERS = { wave: 1, bounce: 1, cascade: 1 };
    var GL = '!<>-_/[]{}=+*^?#abcdefghijklmnopqrstuvwxyz0123456789';
    var PALETTE = [
      ['#d97757', '#ffb59a', '#b4532f'], ['#3b82f6', '#93c5fd', '#1d4ed8'],
      ['#8b5cf6', '#c4b5fd', '#6d28d9'], ['#10b981', '#6ee7b7', '#047857'],
      ['#ec4899', '#f9a8d4', '#be185d'], ['#f59e0b', '#fde68a', '#b45309'],
      ['#06b6d4', '#67e8f9', '#0e7490'], ['#34d399', '#60a5fa', '#a78bfa'],
      ['#f97316', '#ec4899', '#8b5cf6']
    ];
    var re;
    try { re = new RegExp('^\\s*(?:' + T.pattern + ')', 'i'); } catch (e) { re = /^\s*(?:thinking|reasoning)/i; }
    if (T.dots) ROOT.setAttribute('data-osk-tw-dots', '1');

    function storedMs() {
      try {
        var v = parseFloat(localStorage.getItem(KEY_MS));
        if (v >= 0.3 && v <= 30) return Math.round(v * 1000);
      } catch (e) {}
      return Math.round(T.interval * 1000);
    }
    var baseMs = storedMs();

    function rand(list) { return list[Math.floor(Math.random() * list.length)]; }
    function pack() {
      var p = ROOT.getAttribute('data-osk-tw');
      return !p || p === 'off' ? 'default' : p;
    }
    function sig() {
      return [pack(), ROOT.getAttribute('data-osk-tw-fx'), ROOT.getAttribute('data-osk-tw-font'),
        ROOT.getAttribute('data-osk-tw-color'), ROOT.getAttribute('data-osk-tw-rhythm'), baseMs].join('|');
    }
    function holdFor() {
      return ROOT.getAttribute('data-osk-tw-rhythm') === 'natural'
        ? Math.round(baseMs * (0.6 + Math.random() * 0.8)) : baseMs;
    }
    function pickFx(last) {
      var f = ROOT.getAttribute('data-osk-tw-fx') || 'shimmer';
      if (f !== 'random') return f;
      var n = rand(T.fxPool);
      return n === last ? rand(T.fxPool) : n;
    }
    function pickFont() {
      var f = ROOT.getAttribute('data-osk-tw-font') || 'theme';
      return f === 'random' ? rand(T.fontPool) : f;
    }

    function wordsFor(p) {
      var list = [];
      if (p === 'mix') {
        Object.keys(T.packs).forEach(function (k) { list = list.concat(T.packs[k]); });
      } else if (p === 'custom') {
        list = T.custom.length ? T.custom : T.packs.claude;
      } else {
        list = T.packs[p] || T.packs.claude;
      }
      return list.length ? list : ['Thinking'];
    }
    function shuffle(a) {
      for (var i = a.length - 1; i > 0; i--) {
        var j = Math.floor(Math.random() * (i + 1));
        var t = a[i]; a[i] = a[j]; a[j] = t;
      }
      return a;
    }
    function next(st, list) {
      if (T.order === 'sequential') {
        st.i = (st.i + 1) % list.length;
        return list[st.i];
      }
      if (!st.deck.length || st.deckPack !== st.pack) {
        st.deck = shuffle(list.slice());
        st.deckPack = st.pack;
      }
      var w = st.deck.pop();
      if (w === st.last && st.deck.length) { var alt = st.deck.pop(); st.deck.unshift(w); w = alt; }
      st.last = w;
      return w;
    }

    // Decode effect: random glyphs resolve into the word. Uses a text node's
    // nodeValue so the page's MutationObserver is not woken on every frame.
    function scramble(node, text) {
      var tn = document.createTextNode('');
      node.textContent = '';
      node.appendChild(tn);
      var chars = Array.from(text);
      var n = chars.length;
      var total = 12 + n;
      var f = 0;
      var id = setInterval(function () {
        f++;
        if (f >= total || !tn.parentNode) { tn.nodeValue = text; clearInterval(id); return; }
        var reveal = Math.floor((f / total) * n * 1.2);
        var out = '';
        for (var i = 0; i < n; i++) out += (i < reveal || chars[i] === ' ') ? chars[i] : GL.charAt(Math.floor(Math.random() * GL.length));
        tn.nodeValue = out;
      }, 45);
    }

    function build(node, text, fx) {
      node.setAttribute('data-text', text);
      node.style.setProperty('--n', String(Math.max(1, text.length)));
      if (LETTERS[fx]) {
        node.textContent = '';
        var chars = Array.from(text);
        for (var i = 0; i < chars.length; i++) {
          var l = document.createElement('span');
          l.className = 'osk-l';
          l.style.setProperty('--i', String(i));
          l.textContent = chars[i] === ' ' ? '\u00a0' : chars[i];
          node.appendChild(l);
        }
      } else if (fx === 'scramble') {
        scramble(node, text);
      } else {
        node.textContent = text;
      }
    }

    function render(el) {
      var st = el._tw;
      st.pack = pack();
      var fx = pickFx(st.fx);
      st.fx = fx;
      var w = document.createElement('span');
      w.className = 'osk-tw-w';
      w.setAttribute('aria-hidden', 'true');
      w.setAttribute('dir', 'auto');
      w.setAttribute('data-fx', fx);
      w.setAttribute('data-font', pickFont());
      st.hold = holdFor();
      w.style.setProperty('--osk-tw-dur', st.hold + 'ms');
      build(w, next(st, wordsFor(st.pack)), fx);
      if (ROOT.getAttribute('data-osk-tw-color') === 'random') {
        var c = rand(PALETTE);
        w.style.setProperty('--tw-a', c[0]);
        w.style.setProperty('--tw-b', c[1]);
        w.style.setProperty('--tw-c', c[2]);
      }
      el.setAttribute('data-clip', fx === 'slide' ? '1' : '');
      if (st.w && st.w.parentNode === el) el.replaceChild(w, st.w);
      else el.insertBefore(w, st.dots);
      st.w = w;
      st.sig = sig();
      st.at = Date.now();
    }

    function findLabel(svg) {
      var header = svg.closest('button[aria-expanded]') || svg.closest('.shimmer') || svg.parentElement;
      if (!header) return null;
      var walker = document.createTreeWalker(header, NodeFilter.SHOW_TEXT, null);
      var node;
      while ((node = walker.nextNode())) {
        var p = node.parentElement;
        if (!p || p.closest('svg, .osk-th, .osk-tw, .osk-orig')) continue;
        var t = (node.nodeValue || '').trim();
        if (t && re.test(t)) return node;
      }
      return null;
    }

    function mount(svg, node) {
      var orig = document.createElement('span');
      orig.className = 'osk-orig';
      node.parentNode.insertBefore(orig, node);
      orig.appendChild(node);
      var el = document.createElement('span');
      el.className = 'osk-tw';
      var sr = document.createElement('span');
      sr.className = 'osk-sr';
      sr.textContent = (node.nodeValue || '').trim();
      var dots = document.createElement('span');
      dots.className = 'osk-tw-d';
      dots.setAttribute('aria-hidden', 'true');
      dots.innerHTML = '<i></i><i></i><i></i>';
      var tm = document.createElement('span');
      tm.className = 'osk-tw-t';
      tm.setAttribute('aria-hidden', 'true');
      var tt = document.createTextNode('0s');
      tm.appendChild(tt);
      el.appendChild(sr);
      el.appendChild(dots);
      el.appendChild(tm);
      orig.parentNode.insertBefore(el, orig.nextSibling);
      var host = el.closest('.shimmer');
      if (host) host.classList.add('osk-nosh');
      el._tw = { i: -1, deck: [], deckPack: '', last: '', fx: '', start: Date.now(), dots: dots, w: null, tm: tt, tmv: '', sig: '', at: 0, hold: baseMs, pack: pack() };
      el._oskSvg = svg;
      el._oskNode = node;
      el._oskOrig = orig;
      el._oskHost = host;
      svg._oskTwEl = el;
      render(el);
    }

    function teardown(el) {
      var node = el._oskNode;
      var orig = el._oskOrig;
      if (orig && orig.parentNode) {
        if (node && node.parentNode === orig) orig.parentNode.insertBefore(node, orig);
        orig.remove();
      }
      if (el._oskHost) el._oskHost.classList.remove('osk-nosh');
      if (el._oskSvg) el._oskSvg._oskTwEl = null;
      el.remove();
    }

    function tick() {
      var els = document.querySelectorAll('.osk-tw');
      if (!els.length) { clearInterval(timer); timer = null; return; }
      var now = Date.now();
      var timerOn = ROOT.getAttribute('data-osk-tw-timer') === 'on';
      var s0 = sig();
      for (var i = 0; i < els.length; i++) {
        var st = els[i]._tw;
        if (!st) continue;
        if (now - st.at >= st.hold || st.sig !== s0) render(els[i]);
        if (timerOn) {
          var s = Math.floor((now - st.start) / 1000);
          var v = s < 60 ? s + 's' : Math.floor(s / 60) + 'm ' + ('0' + (s % 60)).slice(-2) + 's';
          if (v !== st.tmv) { st.tmv = v; st.tm.nodeValue = v; }
        }
      }
    }

    // Called from scan() with every Thinking spinner svg currently on the page.
    function sync(svgs) {
      var on = pack() !== 'default';
      if (on) {
        for (var i = 0; i < svgs.length; i++) {
          var svg = svgs[i];
          if (svg._oskTwEl && svg._oskTwEl.isConnected) continue;
          var sheet = svg.querySelector('style');
          if (!sheet || sheet.textContent.indexOf('spinner_') === -1) continue;
          var node = findLabel(svg);
          if (node) mount(svg, node);
        }
      }
      var els = document.querySelectorAll('.osk-tw');
      for (var j = 0; j < els.length; j++) {
        var el = els[j];
        if (!on || !el._oskSvg || !el._oskSvg.isConnected) teardown(el);
      }
      var origs = document.querySelectorAll('.osk-orig');
      for (var k = 0; k < origs.length; k++) if (!origs[k].firstChild) origs[k].remove();
      if (!timer && document.querySelector('.osk-tw')) timer = setInterval(tick, 200);
    }

    // ---- settings rows -------------------------------------------------
    function after(prevId) {
      return function () {
        var prev = document.getElementById(prevId);
        var label = document.getElementById(prevId + '-label');
        if (!prev || !prev.parentElement || !label) return null;
        return { parent: prev.parentElement, before: prev.nextSibling, rowClass: label.parentElement.className, nameClass: label.className };
      };
    }

    // Dropdown previews replay every 2.6s so each effect shows its real motion.
    function restart(s) {
      build(s, s.getAttribute('data-text') || 'Thinking', s.getAttribute('data-fx'));
      s.style.animation = 'none';
      void s.offsetWidth;
      s.style.animation = '';
    }
    function pvLoop() {
      if (pvTimer) return;
      pvTimer = setInterval(function () {
        var list = document.querySelectorAll('.osk-pv-s[data-fx]');
        if (!list.length) { clearInterval(pvTimer); pvTimer = null; return; }
        for (var i = 0; i < list.length; i++) restart(list[i]);
      }, 2600);
    }
    function pv(text, attrs, fx) {
      var el = document.createElement('span');
      el.className = 'osk-pv osk-pv-t';
      el.setAttribute('aria-hidden', 'true');
      var s = document.createElement('span');
      s.className = 'osk-pv-s';
      for (var k in attrs) if (Object.prototype.hasOwnProperty.call(attrs, k)) s.setAttribute(k, attrs[k]);
      if (fx) { build(s, text, fx); pvLoop(); } else s.textContent = text;
      el.appendChild(s);
      return el;
    }
    var DICE = '\uD83C\uDFB2';
    function add(id, key, attr, names, fallback, title, note, preview, heading, prev) {
      var p = makePicker({
        id: id, key: key, attr: attr, names: names, fallback: fallback, defaultPreview: null,
        title: title, note: note, preview: preview, heading: heading, place: after(prev)
      });
      pickers.push(p);
      return p;
    }

    add('osk-tw-words', 'owui-thinking-words', 'data-osk-tw', T.packNames, T.fallbacks.pack,
      'Thinking Words', 'Rotating words shown instead of the plain \u201cThinking\u2026\u201d label while a model reasons. Saved in this browser.',
      function (shown) { return pv(T.glyphs[shown] || 'Aa', {}); }, 'Thinking label', 'osk-think-setting');
    add('osk-tw-fx', 'owui-thinking-words-fx', 'data-osk-tw-fx', T.fxNames, T.fallbacks.fx,
      'Phrase Effect', 'How each phrase animates in. Random picks a different effect every time the phrase changes.',
      function (shown) { return shown === 'random' ? pv(DICE, {}) : pv('Thinking', { 'data-fx': shown }, shown); }, null, 'osk-tw-words');
    add('osk-tw-font', 'owui-thinking-words-font', 'data-osk-tw-font', T.fontNames, T.fallbacks.font,
      'Phrase Font', 'Typeface for the thinking phrases (fonts already on your device).',
      function (shown) { return shown === 'random' ? pv(DICE, {}) : pv('Aa', { 'data-font': shown }); }, null, 'osk-tw-fx');
    add('osk-tw-color', 'owui-thinking-words-color', 'data-osk-tw-color', T.colorNames, T.fallbacks.color,
      'Phrase Color', 'Color or gradient of the thinking phrases.',
      function (shown) { return pv('Abc', { 'data-color': shown }); }, null, 'osk-tw-font');

    // Slider: how long each word stays (0.4s - 10s).
    var slider = (function () {
      var id = 'osk-tw-interval';
      function fmt(ms) { return (ms / 1000).toFixed(1) + 's'; }
      function hasStored() { try { return localStorage.getItem(KEY_MS) !== null; } catch (e) { return false; } }
      function ensureRow() {
        if (document.getElementById(id)) return;
        var at = after('osk-tw-color')();
        if (!at) return;
        var block = document.createElement('div');
        block.id = id;
        var line = document.createElement('div');
        line.className = at.rowClass || 'flex items-center justify-between gap-2.5';
        var name = document.createElement('div');
        name.id = id + '-label';
        name.className = at.nameClass || 'min-w-0 text-xs text-gray-600 dark:text-gray-400';
        name.textContent = 'Change Interval';
        var right = document.createElement('div');
        right.className = 'osk-range-row';
        var input = document.createElement('input');
        input.type = 'range';
        input.className = 'osk-range';
        input.min = '0.4';
        input.max = '10';
        input.step = '0.1';
        input.value = String(baseMs / 1000);
        input.setAttribute('aria-labelledby', id + '-label');
        var val = document.createElement('span');
        val.className = 'osk-range-v';
        val.textContent = fmt(baseMs);
        var reset = document.createElement('button');
        reset.type = 'button';
        reset.className = 'osk-range-r';
        reset.textContent = 'Reset';
        reset.hidden = !hasStored();
        input.addEventListener('input', function () {
          var v = parseFloat(input.value);
          baseMs = Math.round(v * 1000);
          try { localStorage.setItem(KEY_MS, String(v)); } catch (e) {}
          val.textContent = fmt(baseMs);
          reset.hidden = false;
        });
        reset.addEventListener('click', function () {
          try { localStorage.removeItem(KEY_MS); } catch (e) {}
          baseMs = storedMs();
          input.value = String(baseMs / 1000);
          val.textContent = fmt(baseMs);
          reset.hidden = true;
        });
        right.appendChild(input);
        right.appendChild(val);
        right.appendChild(reset);
        line.appendChild(name);
        line.appendChild(right);
        var note = document.createElement('p');
        note.className = 'mt-1.5 text-[0.6875rem] text-gray-400 dark:text-gray-600';
        note.textContent = 'How long each thinking phrase stays on screen. Drag left for faster, right for slower.';
        block.appendChild(line);
        block.appendChild(note);
        at.parent.insertBefore(block, at.before);
      }
      window.addEventListener('storage', function (evt) { if (evt.key === KEY_MS) baseMs = storedMs(); });
      return { check: function () {}, ensureRow: ensureRow, apply: function () { baseMs = storedMs(); } };
    })();
    pickers.push(slider);

    add('osk-tw-rhythm', 'owui-thinking-words-rhythm', 'data-osk-tw-rhythm', T.rhythmNames, T.fallbacks.rhythm,
      'Change Rhythm', 'Steady changes at exactly the interval; Natural varies each phrase by up to 40%.',
      function (shown) { return pv(shown === 'natural' ? '| || |' : '| | |', {}); }, null, 'osk-tw-interval');
    add('osk-tw-timer', 'owui-thinking-words-timer', 'data-osk-tw-timer', T.timerNames, T.fallbacks.timer,
      'Thinking Timer', 'Show a live seconds counter next to the words.',
      function (shown) { return pv(shown === 'on' ? '0:07' : '\u2014', {}); }, null, 'osk-tw-rhythm');

    // Console helper: window.__owuiThinkingWords.dump() prints the Thinking header HTML.
    window.__owuiThinkingWords = {
      dump: function () {
        var out = [];
        var svgs = document.querySelectorAll('button[aria-expanded] svg, .shimmer svg');
        for (var i = 0; i < svgs.length; i++) {
          var st = svgs[i].querySelector('style');
          if (!st || st.textContent.indexOf('spinner_') === -1) continue;
          var h = svgs[i].closest('button[aria-expanded]') || svgs[i].parentElement;
          out.push(h.outerHTML.replace(/<style>[\s\S]*?<\/style>/g, '<style>\u2026</style>'));
        }
        console.log(out.join('\n\n') || 'No Thinking header with a spinner on screen right now.');
        return out;
      }
    };

    return {
      pickers: pickers,
      scan: sync,
      apply: function () { pickers.forEach(function (p) { p.apply(); }); }
    };
  })();

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
    message.reroll();
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
    think.reroll();
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
    TW.scan(spinners);
    // The spinner is removed when thinking ends; drop the element we added beside it.
    var added = document.querySelectorAll('.osk-th');
    for (var m = 0; m < added.length; m++) {
      if (!added[m]._oskSvg || !added[m]._oskSvg.isConnected) added[m].remove();
    }
    message.check();
    think.check();
    TW.pickers.forEach(function (p) { p.check(); });
    message.ensureRow();
    think.ensureRow();
    TW.pickers.forEach(function (p) { p.ensureRow(); });
  }

  function schedule() {
    if (scheduled) return;
    scheduled = true;
    requestAnimationFrame(scan);
  }

  ensureStyle();
  message.apply();
  think.apply();
  TW.apply();
  new MutationObserver(schedule).observe(ROOT, { childList: true, subtree: true });
  schedule();
})();
"""


class Event:
    class Valves(BaseModel):
        default_style: STYLE_LITERAL = Field(
            default="default",
            description="What Default means in the Message Loading Indicator setting for users who have not picked a style. 'default' keeps Open WebUI's own indicator and 'random' picks a different style for every message. Others: cursor, dot, bars (legacy loading bars), block (terminal block cursor), typing, wave (equalizer), spinner, shimmer (skeleton lines), ellipsis, scanner, orbit, ripple, breathe (breathing orb), grid (dot grid), flip (flipping square), sparkle, heartbeat, ball (bouncing ball).",
        )
        default_thinking_style: THINK_LITERAL = Field(
            default="default",
            description="What Default means in the Thinking Indicator setting, the icon beside the Thinking header while a model reasons. 'default' keeps Open WebUI's own spinner and 'none' hides the icon. The other values match the message styles of the same name.",
        )

        default_thinking_words: TW_PACK_LITERAL = Field(
            default="claude",
            description="What Default means in the Thinking Words setting (the rotating words that replace the plain 'Thinking...' label). Packs: claude, dev, playful, science, hacker, zen, minimal, mix (everything), custom (uses custom_words below). 'off' keeps Open WebUI's original text.",
        )
        default_words_effect: TW_FX_LITERAL = Field(
            default="shimmer",
            description="Default animation for the thinking words: random (changes every phrase), shimmer, gradient, rainbow, neon, glow, fade, slide, typewriter, scramble, blur, wave, bounce, cascade, glitch, pop, flip, reveal, pulse, plain.",
        )
        default_words_font: TW_FONT_LITERAL = Field(
            default="theme",
            description="Default font for the thinking words: random, theme, mono, serif, italic, rounded, hand, caps, small.",
        )
        default_words_color: TW_COLOR_LITERAL = Field(
            default="orange",
            description="Default color for the thinking words: random (changes every phrase), theme, orange, blue, purple, green, pink, gold, cyan, aurora, sunset.",
        )
        default_words_interval: float = Field(
            default=2.4,
            ge=0.4,
            le=15.0,
            description="Default seconds each thinking phrase stays on screen. Users can change it with the slider in Settings > Interface.",
        )
        default_words_rhythm: TW_RHYTHM_LITERAL = Field(
            default="steady",
            description="'steady' changes phrases at exactly the interval; 'natural' varies each one by up to 40%.",
        )
        default_words_timer: Literal["off", "on"] = Field(
            default="off",
            description="Default for the live seconds counter shown next to the thinking words.",
        )
        words_order: Literal["shuffle", "sequential"] = Field(
            default="shuffle",
            description="'shuffle' plays every word once in random order before repeating; 'sequential' plays them in list order.",
        )
        animated_dots: bool = Field(
            default=True,
            description="Show animated trailing dots after each thinking word.",
        )
        custom_words: str = Field(
            default="",
            description="Your own thinking words, one per line. Used by the 'Custom' pack (and falls back to Claude-style when empty).",
        )
        label_pattern: str = Field(
            default="thinking|reasoning",
            description="Regex (case-insensitive) matching the start of Open WebUI's thinking label. Add your UI language here, e.g. 'thinking|reasoning|pensando|denkt'. Only labels that match are replaced.",
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

    def _custom_words(self) -> list:
        lines = [ln.strip() for ln in (self.valves.custom_words or "").splitlines()]
        return [ln[:60] for ln in lines if ln][:200]

    def _loader_fragment(self) -> str:
        valves = self.valves
        dump = valves.model_dump() if hasattr(valves, "model_dump") else valves.dict()
        cache_key = _json.dumps(dump, sort_keys=True, default=str)
        cached = Event._fragment_cache
        if cached and cached[0] == cache_key:
            return cached[1]
        config = _json.dumps(
            {
                "fallback": valves.default_style,
                "thinkFallback": valves.default_thinking_style,
                "names": STYLE_NAMES,
                "thinkNames": THINK_NAMES,
                "tw": {
                    "packs": TW_PACKS,
                    "custom": self._custom_words(),
                    "order": valves.words_order,
                    "dots": bool(valves.animated_dots),
                    "pattern": valves.label_pattern or "thinking|reasoning",
                    "glyphs": TW_GLYPHS,
                    "fxPool": TW_FX_POOL,
                    "fontPool": TW_FONT_POOL,
                    "interval": float(valves.default_words_interval),
                    "packNames": TW_PACK_NAMES,
                    "fxNames": TW_FX_NAMES,
                    "fontNames": TW_FONT_NAMES,
                    "colorNames": TW_COLOR_NAMES,
                    "rhythmNames": TW_RHYTHM_NAMES,
                    "timerNames": TW_TIMER_NAMES,
                    "fallbacks": {
                        "pack": valves.default_thinking_words,
                        "fx": valves.default_words_effect,
                        "font": valves.default_words_font,
                        "color": valves.default_words_color,
                        "rhythm": valves.default_words_rhythm,
                        "timer": valves.default_words_timer,
                    },
                },
            }
        )
        js = (
            LOADER_SCRIPT.strip()
            .replace("__CONFIG__", config)
            .replace("__CSS__", _json.dumps(OSK_CSS.strip()))
            .replace("__MORE_MARKUP__", _json.dumps(MORE_MARKUP))
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
