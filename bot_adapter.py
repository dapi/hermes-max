"""
MAX messenger Platform Adapter for Hermes Agent — official Bot API mode.

Talks to the MAX Bot API (https://dev.max.ru/docs-api) with a bot token:
  - incoming: long polling ``GET /updates`` (no public HTTPS endpoint needed);
  - outgoing: ``POST /messages`` (markdown, split into <=4000-char parts),
    ``PUT /messages`` for edits, ``POST /chats/{id}/actions`` for typing;
  - media: inbound attachments are downloaded into the Hermes media cache,
    outbound files go through ``POST /uploads``.

The only dependency is httpx, which Hermes already ships — no maxlib/pymax
and no second phone number, unlike the user-account mode in ``adapter.py``.

Configuration in config.yaml::

    platforms:
      max:
        enabled: true
        extra:
          mode: bot                 # optional: implied when a bot token is set
          max_message_length: 4000  # optional

Environment variables:
    MAX_BOT_TOKEN       bot token from the MAX bot settings (required)
    MAX_API_BASE_URL    API base (default https://platform-api.max.ru)
    MAX_CA_BUNDLE       PEM bundle for hosts signed by the Russian Trusted CA
    MAX_HOME_CHANNEL, MAX_ALLOWED_USERS, MAX_ALLOW_ALL_USERS  — as in user mode
"""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import mimetypes
import os
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    MessageEvent,
    MessageType,
)
from gateway.config import Platform

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:  # pragma: no cover - httpx is a Hermes core dependency
    httpx = None  # type: ignore[assignment]
    HTTPX_AVAILABLE = False

logger = logging.getLogger(__name__)

DEFAULT_API_BASE = "https://platform-api2.max.ru"
MAX_TEXT_LIMIT = 4000
POLL_TIMEOUT_SECONDS = 30
POLL_BACKOFF = (1, 2, 5, 10, 30, 60)
UPDATE_TYPES = "message_created,bot_started"
WEBHOOK_UPDATE_TYPES = ["message_created", "bot_started"]
DEFAULT_WEBHOOK_HOST = "0.0.0.0"
DEFAULT_WEBHOOK_PORT = 8645
DEFAULT_WEBHOOK_PATH = "/max/webhook"
# With no saved marker (first run) MAX replays its backlog; drop anything older.
FIRST_RUN_MAX_AGE_SECONDS = 300
ATTACHMENT_RETRY_DELAYS = (1.0, 2.0, 4.0, 8.0)
INBOUND_DOWNLOAD_RETRY_DELAYS = (0.5, 1.5, 3.0)

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".heic"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm"}
AUDIO_EXTENSIONS = {".mp3", ".m4a", ".ogg", ".opus", ".wav", ".aac", ".flac"}

# MAX attachment type -> (Hermes message type, cache kind, fallback extension)
_INBOUND_KINDS = {
    "image": (MessageType.PHOTO, "image", ".jpg"),
    "video": (MessageType.VIDEO, "video", ".mp4"),
    "audio": (MessageType.VOICE, "audio", ".ogg"),
    "file": (MessageType.DOCUMENT, None, ""),
}


async def _fetch_attachment_bytes(
    client: Any,
    url: str,
    *,
    media_kind: str,
    retry_delays: Tuple[float, ...] = INBOUND_DOWNLOAD_RETRY_DELAYS,
) -> Tuple[bytes, str]:
    """Fetch an inbound CDN object, retrying temporary transport/server failures."""
    from gateway.platforms.base import validate_inbound_media_size

    delays = list(retry_delays)
    while True:
        chunks: List[bytes] = []
        total = 0
        try:
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                mime = (resp.headers.get("content-type") or "").split(";")[0].strip()
                async for chunk in resp.aiter_bytes():
                    total += len(chunk)
                    validate_inbound_media_size(total, media_type=media_kind)
                    chunks.append(chunk)
            return b"".join(chunks), mime
        except Exception as exc:
            transport_types = tuple(
                cls for cls in (
                    getattr(httpx, "TimeoutException", None),
                    getattr(httpx, "TransportError", None),
                ) if cls is not None
            )
            status_error_type = getattr(httpx, "HTTPStatusError", None) if HTTPX_AVAILABLE else None
            is_transport = bool(transport_types) and isinstance(exc, transport_types)
            status = getattr(getattr(exc, "response", None), "status_code", None)
            is_retryable_status = bool(
                status_error_type
                and isinstance(exc, status_error_type)
                and status is not None
                and (status in (408, 425, 429) or status >= 500)
            )
            if not is_transport and not is_retryable_status:
                raise
            if not delays:
                raise
            delay = delays.pop(0)
            if is_retryable_status:
                logger.info(
                    "MAX bot: attachment CDN returned %s, retry in %.1fs",
                    status,
                    delay,
                )
            else:
                logger.info(
                    "MAX bot: temporary attachment download failure (%s), retry in %.1fs",
                    type(exc).__name__,
                    delay,
                )
            await asyncio.sleep(delay)


class MaxApiError(Exception):
    """Non-2xx answer from the MAX Bot API."""

    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"MAX API {status} {code}: {message}".strip())
        self.status = status
        self.code = code
        self.message = message


# ---------------------------------------------------------------------------
# Credentials / config
# ---------------------------------------------------------------------------


def _scoped(name: str, default: Any = None) -> Any:
    """Profile-scope-aware secret read (same contract as the user-mode adapter)."""
    from agent.secret_scope import UnscopedSecretError, get_secret

    try:
        val = get_secret(name, default)
    except UnscopedSecretError:
        val = os.getenv(name)
    return val if val is not None else default


def resolve_bot_token(extra: Optional[dict] = None) -> Optional[str]:
    token = _scoped("MAX_BOT_TOKEN") or (extra or {}).get("bot_token")
    return str(token).strip() or None if token else None


def _api_base(extra: dict) -> str:
    return str(os.getenv("MAX_API_BASE_URL") or extra.get("api_base_url") or DEFAULT_API_BASE).rstrip("/")


def _verify(extra: dict) -> Any:
    """TLS verification: a custom CA bundle when configured, else the default store."""
    bundle = os.getenv("MAX_CA_BUNDLE") or extra.get("ca_bundle")
    if bundle:
        path = Path(str(bundle)).expanduser()
        if path.is_file():
            return str(path)
        logger.warning("MAX bot: CA bundle %s not found, using the default trust store", path)
    return True


def _new_client(token: str, extra: dict) -> "httpx.AsyncClient":
    return httpx.AsyncClient(
        base_url=_api_base(extra),
        headers={"Authorization": token},
        verify=_verify(extra),
        timeout=httpx.Timeout(20.0, connect=15.0),
    )


def _raise_for_api(resp: "httpx.Response") -> None:
    if resp.status_code < 300:
        return
    code, message = "", resp.text[:300]
    try:
        body = resp.json()
        if isinstance(body, dict):
            code = str(body.get("code") or "")
            message = str(body.get("message") or message)
    except ValueError:
        pass
    raise MaxApiError(resp.status_code, code, message)


def _ms_to_dt(value: Any) -> datetime:
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc)
    if ts > 10_000_000_000:
        ts /= 1000
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _user_name(user: Optional[dict]) -> Optional[str]:
    if not user:
        return None
    name = user.get("name")
    if name:
        return str(name)
    full = " ".join(p for p in (user.get("first_name"), user.get("last_name")) if p)
    return full or user.get("username") or None


def _upload_type_for(path: Path, default: str = "file") -> str:
    suffix = path.suffix.lower()
    if suffix in IMAGE_EXTENSIONS:
        return "image"
    if suffix in VIDEO_EXTENSIONS:
        return "video"
    if suffix in AUDIO_EXTENSIONS:
        return "audio"
    return default


# ---------------------------------------------------------------------------
# Stateless API helpers (shared by the live adapter and the cron sender)
# ---------------------------------------------------------------------------


async def _post_message(
    client: "httpx.AsyncClient",
    chat_id: str,
    body: Dict[str, Any],
) -> Optional[str]:
    """POST /messages with retries while an uploaded attachment is processed.

    Returns the new message id (``mid``). A markdown part MAX rejects is
    re-sent as plain text so a formatting quirk never loses the answer.
    """
    params = {"chat_id": chat_id}
    delays = list(ATTACHMENT_RETRY_DELAYS)
    while True:
        resp = await client.post("/messages", params=params, json=body)
        if resp.status_code < 300:
            data = resp.json() if resp.content else {}
            message = data.get("message") or {}
            return (message.get("body") or {}).get("mid")
        try:
            _raise_for_api(resp)
        except MaxApiError as exc:
            if exc.code == "attachment.not.ready" and delays:
                await asyncio.sleep(delays.pop(0))
                continue
            if exc.status == 400 and body.get("format"):
                logger.debug("MAX bot: markdown rejected (%s), retrying as plain text", exc)
                body = {k: v for k, v in body.items() if k != "format"}
                continue
            raise


async def _upload(client: "httpx.AsyncClient", path: Path, upload_type: str) -> Dict[str, Any]:
    """Upload a local file; return the attachment payload for POST /messages."""
    resp = await client.post("/uploads", params={"type": upload_type})
    _raise_for_api(resp)
    slot = resp.json()
    upload_url = slot.get("url")
    if not upload_url:
        raise MaxApiError(resp.status_code, "upload.no_url", "MAX returned no upload URL")

    mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    data = await asyncio.to_thread(path.read_bytes)
    # Absolute URL: httpx ignores base_url but still sends the Authorization header.
    up = await client.post(
        upload_url,
        files={"data": (path.name, data, mime)},
        timeout=httpx.Timeout(300.0, connect=15.0),
    )
    _raise_for_api(up)

    payload: Dict[str, Any] = {}
    try:
        parsed = up.json()
        if isinstance(parsed, dict):
            payload = parsed
    except ValueError:
        pass  # video/audio answer with a non-JSON body; the token came with the slot
    if "token" not in payload and "photos" not in payload:
        if not slot.get("token"):
            raise MaxApiError(up.status_code, "upload.no_token", "MAX upload returned no token")
        payload = {"token": slot["token"]}
    return payload


async def _send_parts(
    client: "httpx.AsyncClient",
    chat_id: str,
    parts: List[str],
    *,
    reply_to: Optional[str] = None,
    attachments: Optional[List[dict]] = None,
    markdown: bool = True,
) -> List[str]:
    """Send text parts in order; attachments ride on the first part."""
    mids: List[str] = []
    for index, part in enumerate(parts):
        body: Dict[str, Any] = {"text": part}
        if markdown and part:
            body["format"] = "markdown"
        if index == 0:
            if attachments:
                body["attachments"] = attachments
            if reply_to:
                body["link"] = {"type": "reply", "mid": reply_to}
        mid = await _post_message(client, chat_id, body)
        if mid:
            mids.append(mid)
    return mids


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class MaxBotAdapter(BasePlatformAdapter):
    """MAX Bot API adapter: long polling in, REST out."""

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform("max"))
        self._extra: dict = getattr(config, "extra", {}) or {}
        self._token: Optional[str] = resolve_bot_token(self._extra)
        self.max_message_length = min(
            int(self._extra.get("max_message_length") or MAX_TEXT_LIMIT), MAX_TEXT_LIMIT
        )
        self._markdown = str(self._extra.get("markdown", True)).lower() not in ("false", "0", "no")
        self._webhook_url = str(
            os.getenv("MAX_WEBHOOK_URL") or self._extra.get("webhook_url") or ""
        ).strip()
        self._webhook_secret = str(
            os.getenv("MAX_WEBHOOK_SECRET") or self._extra.get("webhook_secret") or ""
        ).strip()
        self._webhook_host = str(
            os.getenv("MAX_WEBHOOK_HOST") or self._extra.get("webhook_host") or DEFAULT_WEBHOOK_HOST
        ).strip()
        self._webhook_port = int(
            os.getenv("MAX_WEBHOOK_PORT") or self._extra.get("webhook_port") or DEFAULT_WEBHOOK_PORT
        )
        path = str(
            os.getenv("MAX_WEBHOOK_PATH") or self._extra.get("webhook_path") or DEFAULT_WEBHOOK_PATH
        ).strip()
        self._webhook_path = path if path.startswith("/") else f"/{path}"

        self._client: Optional["httpx.AsyncClient"] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._webhook_runner: Any = None
        self._me_id: Optional[int] = None
        self._me_name: Optional[str] = None
        self._marker: Optional[int] = None
        self._first_run = False
        self._started_at = time.time()
        self._seen: deque = deque(maxlen=2000)
        self._chat_types: Dict[str, str] = {}

    @property
    def name(self) -> str:
        return "MAX"

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not HTTPX_AVAILABLE:
            self._set_fatal_error("deps_missing", "httpx is not installed", retryable=False)
            return False
        if not self._token:
            logger.error("MAX bot: no token — set MAX_BOT_TOKEN")
            self._set_fatal_error(
                "config_missing", "MAX bot token not found (MAX_BOT_TOKEN)", retryable=False
            )
            return False

        self._client = _new_client(self._token, self._extra)
        try:
            resp = await self._client.get("/me")
            _raise_for_api(resp)
            me = resp.json()
        except MaxApiError as exc:
            await self._close_client()
            retryable = exc.status not in (401, 403)
            logger.error("MAX bot: /me failed: %s", exc)
            self._set_fatal_error(
                "unauthorized" if not retryable else "connect_failed", str(exc), retryable=retryable
            )
            return False
        except Exception as exc:
            await self._close_client()
            logger.error("MAX bot: connect failed: %s", exc)
            self._set_fatal_error("connect_failed", str(exc), retryable=True)
            return False

        self._me_id = me.get("user_id")
        self._me_name = me.get("name") or me.get("username")
        self._started_at = time.time()
        self._marker = self._load_marker()
        self._first_run = self._marker is None

        self._mark_connected()
        if self._webhook_url:
            if not self._webhook_secret:
                await self._close_client()
                self._set_fatal_error(
                    "config_missing", "MAX webhook mode requires MAX_WEBHOOK_SECRET", retryable=False
                )
                return False
            try:
                await self._start_webhook_listener()
                await self._ensure_webhook_subscription()
            except Exception as exc:
                await self._stop_webhook_listener()
                await self._close_client()
                logger.error("MAX bot: webhook setup failed: %s", exc)
                self._set_fatal_error("connect_failed", str(exc), retryable=True)
                return False
            logger.info(
                "MAX bot: connected as %s (id %s), webhook %s -> %s:%s%s",
                self._me_name,
                self._me_id,
                self._webhook_url,
                self._webhook_host,
                self._webhook_port,
                self._webhook_path,
            )
        else:
            self._poll_task = asyncio.create_task(self._poll_loop())
            logger.info(
                "MAX bot: connected as %s (id %s), long polling %s",
                self._me_name, self._me_id, _api_base(self._extra),
            )
        self._wire_plugin_handlers(None)
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()
        task, self._poll_task = self._poll_task, None
        if task and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        await self._stop_webhook_listener()
        await self._close_client()

    async def _start_webhook_listener(self) -> None:
        from aiohttp import web
        from gateway.platforms.shared_ingress import bind_listener

        app = web.Application(client_max_size=16 * 1024 * 1024)
        app.router.add_get("/health", self._handle_webhook_health)
        app.router.add_post(self._webhook_path, self._handle_webhook)
        self._webhook_runner = await bind_listener(
            self,
            app,
            self._webhook_host or None,
            self._webhook_port,
            self._webhook_path,
            access_log=None,
        )

    async def _stop_webhook_listener(self) -> None:
        runner, self._webhook_runner = self._webhook_runner, None
        if runner is not None:
            try:
                await runner.cleanup()
            except Exception:
                pass

    async def _handle_webhook_health(self, request):
        from aiohttp import web

        return web.json_response({"status": "ok", "platform": "max"})

    async def _handle_webhook(self, request):
        from aiohttp import web

        actual = request.headers.get("X-Max-Bot-Api-Secret", "")
        if not self._webhook_secret or not hmac.compare_digest(actual, self._webhook_secret):
            return web.json_response({"success": False, "error": "unauthorized"}, status=401)
        try:
            update = await request.json()
        except Exception:
            return web.json_response({"success": False, "error": "invalid json"}, status=400)
        if not isinstance(update, dict):
            return web.json_response({"success": False, "error": "invalid update"}, status=400)
        try:
            await self._handle_update(update)
        except Exception:
            logger.exception("MAX bot: failed to handle webhook update")
            return web.json_response({"success": False}, status=500)
        return web.json_response({"success": True})

    async def _ensure_webhook_subscription(self) -> None:
        if self._client is None or not self._webhook_url:
            return
        resp = await self._client.get("/subscriptions")
        _raise_for_api(resp)
        subscriptions = (resp.json() or {}).get("subscriptions") or []
        for subscription in subscriptions:
            if subscription.get("url") == self._webhook_url:
                return
        resp = await self._client.post(
            "/subscriptions",
            json={
                "url": self._webhook_url,
                "update_types": WEBHOOK_UPDATE_TYPES,
                "secret": self._webhook_secret,
            },
        )
        _raise_for_api(resp)

    async def _close_client(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            try:
                await client.aclose()
            except Exception:
                pass

    # ── Marker persistence (survive restarts without replaying updates) ───

    def _marker_path(self) -> Path:
        from hermes_constants import get_hermes_home

        return Path(get_hermes_home()) / "max_bot_state.json"

    def _load_marker(self) -> Optional[int]:
        try:
            data = json.loads(self._marker_path().read_text(encoding="utf-8"))
            value = (data.get("markers") or {}).get(str(self._me_id))
            return int(value) if value is not None else None
        except (OSError, ValueError, TypeError, AttributeError):
            return None

    def _save_marker(self) -> None:
        if self._marker is None or self._me_id is None:
            return
        path = self._marker_path()
        try:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(data, dict):
                    data = {}
            except (OSError, ValueError):
                data = {}
            data.setdefault("markers", {})[str(self._me_id)] = self._marker
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(data), encoding="utf-8")
            os.replace(tmp, path)
        except OSError as exc:
            logger.debug("MAX bot: could not persist marker: %s", exc)

    # ── Long polling ──────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        backoff_idx = 0
        while self._running and self._client is not None:
            params: Dict[str, Any] = {
                "timeout": POLL_TIMEOUT_SECONDS,
                "limit": 100,
                "types": UPDATE_TYPES,
            }
            if self._marker is not None:
                params["marker"] = self._marker
            try:
                resp = await self._client.get(
                    "/updates",
                    params=params,
                    timeout=httpx.Timeout(POLL_TIMEOUT_SECONDS + 15.0, connect=15.0),
                )
                if resp.status_code in (401, 403):
                    logger.error("MAX bot: token rejected (%s) — stopping", resp.status_code)
                    self._set_fatal_error(
                        "unauthorized", "MAX rejected the bot token", retryable=False
                    )
                    return
                _raise_for_api(resp)
                data = resp.json()
                backoff_idx = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._running:
                    return
                delay = POLL_BACKOFF[min(backoff_idx, len(POLL_BACKOFF) - 1)]
                backoff_idx += 1
                logger.warning("MAX bot: polling error (%s), retry in %ss", exc, delay)
                await asyncio.sleep(delay)
                continue

            for update in data.get("updates") or []:
                try:
                    await self._handle_update(update)
                except Exception:
                    logger.exception("MAX bot: failed to handle update")

            marker = data.get("marker")
            if marker is not None and marker != self._marker:
                self._marker = marker
                self._save_marker()
            self._first_run = False

    # ── Incoming ──────────────────────────────────────────────────────────

    def _is_stale(self, update: dict) -> bool:
        if not self._first_run:
            return False
        ts = update.get("timestamp")
        try:
            sent = float(ts) / 1000
        except (TypeError, ValueError):
            return False
        return sent < self._started_at - FIRST_RUN_MAX_AGE_SECONDS

    async def _handle_update(self, update: dict) -> None:
        if not self._message_handler:
            return
        if self._is_stale(update):
            logger.info("MAX bot: skipping backlog update from before first start")
            return
        kind = update.get("update_type")
        if kind == "message_created":
            await self._handle_message(update)
        elif kind == "bot_started":
            await self._handle_bot_started(update)

    async def _handle_bot_started(self, update: dict) -> None:
        chat_id = update.get("chat_id")
        user = update.get("user") or {}
        if chat_id is None:
            return
        self._chat_types[str(chat_id)] = "dm"
        user_id = user.get("user_id")
        name = _user_name(user)
        payload = (update.get("payload") or "").strip()
        source = self.build_source(
            chat_id=str(chat_id),
            chat_name=name or str(chat_id),
            chat_type="dm",
            user_id=str(user_id) if user_id is not None else None,
            user_name=name,
        )
        await self.handle_message(
            MessageEvent(
                text=f"/start {payload}".strip(),
                message_type=MessageType.COMMAND,
                source=source,
                user_id=str(user_id) if user_id is not None else None,
                user_name=name,
                raw_message=update,
                timestamp=_ms_to_dt(update.get("timestamp")),
            )
        )

    async def _handle_message(self, update: dict) -> None:
        message = update.get("message") or {}
        body = message.get("body") or {}
        sender = message.get("sender") or {}
        recipient = message.get("recipient") or {}

        chat_id = recipient.get("chat_id")
        if chat_id is None:
            return
        mid = body.get("mid")
        if mid:
            if mid in self._seen:
                return
            self._seen.append(mid)

        sender_id = sender.get("user_id")
        if sender_id is not None and sender_id == self._me_id:
            return  # our own message echoed back

        chat_type = "dm" if recipient.get("chat_type") == "dialog" else "group"
        self._chat_types[str(chat_id)] = chat_type

        text = (body.get("text") or "").strip()
        attachments = list(body.get("attachments") or [])

        link = message.get("link") or {}
        linked = link.get("message") or {}
        reply_mid = reply_text = reply_author_id = reply_author = None
        reply_is_own = False
        if link.get("type") == "forward":
            fwd_from = _user_name(link.get("sender")) or "unknown"
            fwd_text = (linked.get("text") or "").strip()
            header = f"[Forwarded from {fwd_from}]"
            text = "\n".join(p for p in (text, header, fwd_text) if p)
            attachments.extend(linked.get("attachments") or [])
        elif link.get("type") == "reply":
            reply_mid = linked.get("mid")
            reply_text = linked.get("text")
            link_sender = link.get("sender") or {}
            reply_author_id = link_sender.get("user_id")
            reply_author = _user_name(link_sender)
            reply_is_own = reply_author_id is not None and reply_author_id == self._me_id

        media_urls, media_types, notes, msg_type = await self._collect_attachments(attachments)
        if notes:
            text = "\n".join(p for p in (text, " ".join(notes)) if p)
        if not text and not media_urls:
            return
        if not media_urls and text.startswith("/"):
            msg_type = MessageType.COMMAND

        name = _user_name(sender)
        source = self.build_source(
            chat_id=str(chat_id),
            chat_name=name if chat_type == "dm" else str(chat_id),
            chat_type=chat_type,
            user_id=str(sender_id) if sender_id is not None else None,
            user_name=name,
            is_bot=bool(sender.get("is_bot")),
            message_id=mid,
        )
        await self.handle_message(
            MessageEvent(
                text=text,
                message_type=msg_type,
                source=source,
                user_id=str(sender_id) if sender_id is not None else None,
                user_name=name,
                raw_message=update,
                message_id=mid,
                media_urls=media_urls,
                media_types=media_types,
                reply_to_message_id=reply_mid,
                reply_to_text=reply_text,
                reply_to_author_id=str(reply_author_id) if reply_author_id is not None else None,
                reply_to_author_name=reply_author,
                reply_to_is_own_message=reply_is_own,
                timestamp=_ms_to_dt(message.get("timestamp") or update.get("timestamp")),
            )
        )

    async def _collect_attachments(
        self, attachments: List[dict]
    ) -> Tuple[List[str], List[str], List[str], MessageType]:
        """Download cacheable attachments; describe the rest as text notes."""
        media_urls: List[str] = []
        media_types: List[str] = []
        notes: List[str] = []
        msg_type = MessageType.TEXT

        for att in attachments:
            att_type = att.get("type") or "attachment"
            payload = att.get("payload") or {}
            kind = _INBOUND_KINDS.get(att_type)
            url = payload.get("url") if isinstance(payload, dict) else None

            if kind and url:
                hermes_type, cache_kind, default_ext = kind
                filename = att.get("filename") or ""
                try:
                    cached = await self._download_to_cache(
                        url, filename=filename, default_kind=cache_kind, default_ext=default_ext
                    )
                except Exception as exc:
                    logger.warning("MAX bot: could not download %s attachment: %s", att_type, exc)
                    cached = None
                if cached:
                    path, mime = cached
                    media_urls.append(path)
                    media_types.append(mime)
                    if msg_type == MessageType.TEXT:
                        msg_type = hermes_type
                    continue
                notes.append(f"[{att_type}: {filename or 'download failed'}]")
            elif att_type == "location":
                notes.append(f"[location: {att.get('latitude')}, {att.get('longitude')}]")
                if msg_type == MessageType.TEXT:
                    msg_type = MessageType.LOCATION
            elif att_type == "share" and url:
                notes.append(f"[link: {url}]")
            elif att_type == "contact":
                info = payload.get("max_info") or {}
                notes.append(f"[contact: {_user_name(info) or payload.get('vcf_info') or ''}]".replace(": ]", "]"))
            elif att_type == "sticker":
                notes.append("[sticker]")
            elif att_type != "inline_keyboard":
                notes.append(f"[{att_type}]")
        return media_urls, media_types, notes, msg_type

    async def _download_to_cache(
        self, url: str, *, filename: str, default_kind: Optional[str], default_ext: str
    ) -> Optional[Tuple[str, str]]:
        from gateway.platforms.base import cache_media_bytes_async

        # Attachment CDNs are public hosts: no Authorization header, default trust store.
        async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=15.0), follow_redirects=True) as dl:
            data, mime = await _fetch_attachment_bytes(
                dl, url, media_kind=default_kind or "document"
            )
        if not filename and default_ext and not mimetypes.guess_extension(mime or ""):
            filename = f"attachment{default_ext}"
        cached = await cache_media_bytes_async(
            data, filename=filename, mime_type=mime, default_kind=default_kind
        )
        if cached is None:
            return None
        return cached.path, cached.media_type

    # ── Sending ───────────────────────────────────────────────────────────

    def _send_failure(self, chat_id: str, exc: Exception) -> SendResult:
        logger.warning("MAX bot: send to %s failed: %s", chat_id, exc)
        retryable = not isinstance(exc, MaxApiError) or exc.status >= 500 or exc.status == 429
        return SendResult(success=False, error=str(exc), retryable=retryable)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if self._client is None:
            return SendResult(success=False, error="MAX bot: not connected", retryable=True)
        parts = self.truncate_message(content or "", self.max_message_length)
        parts = [p for p in parts if p.strip()]
        if not parts:
            return SendResult(success=False, error="MAX bot: empty message")
        try:
            mids = await _send_parts(
                self._client, str(chat_id), parts, reply_to=reply_to, markdown=self._markdown
            )
        except Exception as exc:
            return self._send_failure(chat_id, exc)
        return SendResult(
            success=True,
            message_id=mids[-1] if mids else None,
            continuation_message_ids=tuple(mids[:-1]),
        )

    async def edit_message(
        self, chat_id: str, message_id: str, content: str, *, finalize: bool = False
    ) -> SendResult:
        if self._client is None or not message_id:
            return SendResult(success=False, error="MAX bot: not connected")
        if len(content or "") > self.max_message_length:
            return SendResult(success=False, error="MAX bot: edit exceeds message limit")
        body: Dict[str, Any] = {"text": content}
        if self._markdown:
            body["format"] = "markdown"
        try:
            resp = await self._client.put("/messages", params={"message_id": message_id}, json=body)
            if resp.status_code == 400 and "format" in body:
                resp = await self._client.put(
                    "/messages", params={"message_id": message_id}, json={"text": content}
                )
            _raise_for_api(resp)
        except Exception as exc:
            return SendResult(success=False, error=str(exc))
        return SendResult(success=True, message_id=message_id)

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        if self._client is None or not message_id:
            return False
        try:
            resp = await self._client.delete("/messages", params={"message_id": message_id})
            _raise_for_api(resp)
            return True
        except Exception as exc:
            logger.debug("MAX bot: delete failed: %s", exc)
            return False

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        if self._client is None:
            return
        try:
            await self._client.post(f"/chats/{chat_id}/actions", json={"action": "typing_on"})
        except Exception:
            pass  # cosmetic — never let a typing hiccup surface

    async def _send_file(
        self,
        chat_id: str,
        path: str,
        caption: Optional[str],
        reply_to: Optional[str],
        *,
        upload_type: Optional[str] = None,
    ) -> SendResult:
        if self._client is None:
            return SendResult(success=False, error="MAX bot: not connected", retryable=True)
        file_path = Path(path).expanduser()
        if not file_path.is_file():
            return SendResult(success=False, error=f"MAX bot: file not found: {file_path}")
        kind = upload_type or _upload_type_for(file_path)
        try:
            payload = await _upload(self._client, file_path, kind)
            parts = self.truncate_message(caption or "", self.max_message_length) if caption else [""]
            mids = await _send_parts(
                self._client,
                str(chat_id),
                parts or [""],
                reply_to=reply_to,
                attachments=[{"type": kind, "payload": payload}],
                markdown=self._markdown,
            )
        except Exception as exc:
            return self._send_failure(chat_id, exc)
        return SendResult(success=True, message_id=mids[-1] if mids else None)

    async def send_image_file(
        self, chat_id: str, image_path: str, caption: Optional[str] = None,
        reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None, **kwargs,
    ) -> SendResult:
        return await self._send_file(chat_id, image_path, caption, reply_to, upload_type="image")

    async def send_document(
        self, chat_id: str, file_path: str, caption: Optional[str] = None,
        file_name: Optional[str] = None, reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None, **kwargs,
    ) -> SendResult:
        return await self._send_file(chat_id, file_path, caption, reply_to, upload_type="file")

    async def send_video(
        self, chat_id: str, video_path: str, caption: Optional[str] = None,
        reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None, **kwargs,
    ) -> SendResult:
        return await self._send_file(chat_id, video_path, caption, reply_to, upload_type="video")

    async def send_voice(
        self, chat_id: str, audio_path: str, caption: Optional[str] = None,
        reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None, **kwargs,
    ) -> SendResult:
        return await self._send_file(chat_id, audio_path, caption, reply_to, upload_type="audio")

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        info = {
            "name": str(chat_id),
            "type": self._chat_types.get(str(chat_id), "dm"),
            "chat_id": str(chat_id),
        }
        if self._client is None:
            return info
        try:
            resp = await self._client.get(f"/chats/{chat_id}")
            _raise_for_api(resp)
            chat = resp.json()
        except Exception:
            return info
        info["type"] = "dm" if chat.get("type") == "dialog" else "group"
        info["name"] = (
            chat.get("title") or _user_name(chat.get("dialog_with_user")) or info["name"]
        )
        return info


# ---------------------------------------------------------------------------
# Out-of-process send (cron delivery without a live gateway adapter)
# ---------------------------------------------------------------------------


async def standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    media_files: Optional[List[str]] = None,
) -> Dict[str, Any]:
    extra = getattr(pconfig, "extra", {}) or {}
    token = resolve_bot_token(extra)
    if not token:
        return {"error": "MAX standalone send: MAX_BOT_TOKEN is not set"}
    if not HTTPX_AVAILABLE:
        return {"error": "MAX standalone send: httpx not installed"}
    markdown = str(extra.get("markdown", True)).lower() not in ("false", "0", "no")
    try:
        async with _new_client(token, extra) as client:
            attachments = []
            for value in media_files or []:
                path = Path(value).expanduser()
                kind = _upload_type_for(path)
                attachments.append({"type": kind, "payload": await _upload(client, path, kind)})
            parts = [p for p in BasePlatformAdapter.truncate_message(message or "", MAX_TEXT_LIMIT) if p.strip()]
            if not parts:
                parts = [""]
            mids = await _send_parts(
                client, str(chat_id), parts, attachments=attachments or None, markdown=markdown
            )
        return {
            "success": True,
            "platform": "max",
            "chat_id": str(chat_id),
            "message_id": mids[-1] if mids else None,
        }
    except Exception as exc:
        logger.debug("MAX bot standalone send raised", exc_info=True)
        return {"error": f"MAX standalone send failed: {exc}"}
