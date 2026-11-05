"""
MAX messenger Platform Adapter for Hermes Agent.

Two transports share the ``max`` platform:

  - **bot mode** (``bot_adapter.py``) — official MAX Bot API with a bot token
    (``MAX_BOT_TOKEN``); needs only httpx. Selected when a bot token is set or
    ``extra.mode: bot``.
  - **user-account mode** (this file) — described below.

User-account mode
-----------------

Transport is vendored from maxcli (https://github.com/dapi/maxcli, GPL-3.0):
  - incoming messages: maxlib (0.1b1) WebSocket listener with the local
    Origin-header patch (MAX rejects handshakes without Origin);
  - outgoing messages: maxapi-python (pymax, SocketMaxClient) ephemeral
    client per send batch, serialized with a lock;
  - credentials: shares the maxcli token storage
    (~/.config/maxcli/config.json). Authenticate once with
    ``maxcli auth +7...`` and this adapter reuses the session token.

Configuration in config.yaml::

    platforms:
      max:
        enabled: true
        home_channel:
          platform: max
          chat_id: '123456789'
          name: Me
        extra:
          profile: default          # maxcli profile (optional)
          max_message_length: 4000  # optional

Or via environment variables:
    MAX_TOKEN / MAXCLI_TOKEN, MAX_PHONE / MAXCLI_PHONE,
    MAX_PROFILE / MAXCLI_PROFILE, MAXCLI_CONFIG_DIR,
    MAX_HOME_CHANNEL, MAX_ALLOWED_USERS, MAX_ALLOW_ALL_USERS

Dependencies: maxlib==0.1b1, maxapi-python==1.2.5, lz4, msgpack.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import stat
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from agent.secret_scope import UnscopedSecretError as _UnscopedSecretError
from agent.secret_scope import get_secret as _scoped_get_secret

from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    MessageEvent,
    MessageType,
)
from gateway.config import Platform


def _get_scoped_secret(name, default=None):
    """Scope-aware credential read with default-profile env fallback.

    Same pattern as the IRC adapter / whatsapp_common._get_wsecret.
    """
    try:
        val = _scoped_get_secret(name, default)
    except _UnscopedSecretError:
        val = os.getenv(name)
    return val if val is not None else default


logger = logging.getLogger(__name__)

DEFAULT_PROFILE = "default"
PHOTO_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm", ".matroska"}

# ---------------------------------------------------------------------------
# Vendored maxcli transport helpers (github.com/dapi/maxcli, src/maxcli/cli.py)
# ---------------------------------------------------------------------------


def _load_maxlib() -> Any:
    """Import maxlib and apply the Origin-handshake patch."""
    import maxlib

    max_module = getattr(maxlib, "max", None)
    if max_module is not None and not getattr(max_module, "_maxcli_origin_patch", False):
        original_connect = max_module.connect

        def connect_with_origin(uri: str, *args: Any, **kwargs: Any) -> Any:
            kwargs.setdefault("origin", "https://web.max.ru")
            return original_connect(uri, *args, **kwargs)

        max_module.connect = connect_with_origin
        max_module._maxcli_origin_patch = True
    return maxlib


def _load_pymax() -> Tuple[Any, Any]:
    from pymax import SocketMaxClient
    from pymax.payloads import UserAgentPayload

    return SocketMaxClient, UserAgentPayload


def _patch_pymax_socket_unpack(client: Any) -> None:
    """Vendored from maxcli: robust _unpack_packet (lz4/msgpack) for pymax."""
    import lz4.block
    import msgpack

    def unpack_packet(data: bytes) -> dict[str, Any] | None:
        ver = int.from_bytes(data[0:1], "big")
        cmd = int.from_bytes(data[1:3], "big")
        seq = int.from_bytes(data[3:4], "big")
        opcode = int.from_bytes(data[4:6], "big")
        packed_len = int.from_bytes(data[6:10], "big", signed=False)
        comp_flag = packed_len >> 24
        payload_length = packed_len & 0xFFFFFF
        payload_bytes = data[10 : 10 + payload_length]

        payload = None
        if payload_bytes:
            if comp_flag != 0:
                original_payload = payload_bytes
                for size in (500_000, 2_000_000, 10_000_000, 50_000_000):
                    try:
                        payload_bytes = lz4.block.decompress(
                            original_payload,
                            uncompressed_size=size,
                        )
                        break
                    except lz4.block.LZ4BlockError:
                        continue
                else:
                    return None
            payload = msgpack.unpackb(payload_bytes, raw=False, strict_map_key=False)

        return {
            "ver": ver,
            "cmd": cmd,
            "seq": seq,
            "opcode": opcode,
            "payload": payload,
        }

    client._unpack_packet = unpack_packet


def _quiet_pymax_logger() -> logging.Logger:
    quiet = logging.getLogger("hermes.max.pymax")
    quiet.handlers.clear()
    quiet.addHandler(logging.NullHandler())
    quiet.propagate = False
    quiet.setLevel(logging.CRITICAL + 1)
    return quiet


def _config_home() -> Path:
    override = os.environ.get("MAXCLI_CONFIG_DIR")
    if override:
        return Path(override).expanduser()
    xdg = os.environ.get("XDG_CONFIG_HOME")
    if xdg:
        return Path(xdg).expanduser() / "maxcli"
    return Path.home() / ".config" / "maxcli"


def _load_maxcli_profile(profile_name: str) -> dict[str, Any]:
    path = _config_home() / "config.json"
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(data, dict):
        return {}
    return (data.get("profiles") or {}).get(profile_name) or {}


def _resolve_credentials(extra: dict[str, Any]) -> Tuple[Optional[str], Optional[str], str]:
    """Resolve (token, phone, profile_name) for the MAX account.

    Priority: env vars > config.yaml extra > maxcli config profile.
    """
    profile_name = (
        os.getenv("MAX_PROFILE")
        or os.getenv("MAXCLI_PROFILE")
        or extra.get("profile")
        or DEFAULT_PROFILE
    )
    profile = _load_maxcli_profile(profile_name)
    token = (
        _get_scoped_secret("MAX_TOKEN")
        or _get_scoped_secret("MAXCLI_TOKEN")
        or extra.get("token")
        or profile.get("token")
    )
    phone = (
        os.getenv("MAX_PHONE")
        or os.getenv("MAXCLI_PHONE")
        or extra.get("phone")
        or profile.get("phone")
    )
    return token, phone, profile_name


def _pymax_session_dir() -> Path:
    path = _config_home() / "pymax-session"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _timestamp_to_dt(value: Any) -> datetime:
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc)
    if ts > 10_000_000_000:
        ts = ts / 1000
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _contact_name(contact: Any) -> Optional[str]:
    names = getattr(contact, "names", None) or []
    if names:
        name = getattr(names[0], "name", None)
        if name:
            return name
        first = getattr(names[0], "first_name", None)
        last = getattr(names[0], "last_name", None)
        full = " ".join(part for part in [first, last] if part)
        if full:
            return full
    return None


def _value_from(source: Any, *names: str) -> Any:
    for name in names:
        if isinstance(source, dict) and name in source:
            return source[name]
        if not isinstance(source, dict) and hasattr(source, name):
            return getattr(source, name)
    return None


def _describe_attachment(attach: Any) -> str:
    """Human-readable placeholder for an incoming attachment."""
    att_type = _value_from(attach, "type", "_type") or "attachment"
    att_type = getattr(att_type, "value", att_type)
    name = _value_from(attach, "name", "file_name", "fileName")
    if name:
        return f"[{att_type}: {name}]"
    return f"[{att_type}]"


def _split_text(content: str, limit: int) -> List[str]:
    """Split text into chunks of at most ``limit`` chars on space/newline."""
    chunks: List[str] = []
    for paragraph in content.split("\n"):
        while len(paragraph) > limit:
            split_at = paragraph.rfind(" ", 0, limit)
            if split_at < limit // 3:
                split_at = limit
            chunks.append(paragraph[:split_at].rstrip())
            paragraph = paragraph[split_at:].lstrip()
        chunks.append(paragraph)
    return [c for c in chunks if c.strip()] or [""]


# ---------------------------------------------------------------------------
# pymax ephemeral send (vendored from maxcli pymax_send_message)
# ---------------------------------------------------------------------------


def _build_pymax_attachments(paths: List[str]) -> list:
    if not paths:
        return []
    from pymax.files import File, Photo, Video

    attachments = []
    for value in paths:
        path = Path(value).expanduser()
        if not path.is_file():
            raise FileNotFoundError(f"Attachment not found: {path}")
        suffix = path.suffix.lower()
        if suffix in PHOTO_EXTENSIONS:
            attachments.append(Photo(path=str(path)))
        elif suffix in VIDEO_EXTENSIONS:
            attachments.append(Video(path=str(path)))
        else:
            attachments.append(File(path=str(path)))
    return attachments


async def _pymax_send(
    token: str,
    phone: str,
    chat_id: int,
    chunks: List[str],
    *,
    reply_to: Optional[int] = None,
    media_files: Optional[List[str]] = None,
) -> Optional[str]:
    """Open an ephemeral pymax connection, send chunks, close.

    Mirrors maxcli's per-command connection lifecycle: connect + sync +
    send + cleanup. Serialized by the caller (adapter-level lock).
    """
    SocketMaxClient, UserAgentPayload = _load_pymax()
    client = SocketMaxClient(
        phone=phone,
        token=token,
        work_dir=str(_pymax_session_dir()),
        headers=UserAgentPayload(device_type="DESKTOP", app_version="26.25.0"),
        logger=_quiet_pymax_logger(),
        reconnect=False,
    )
    _patch_pymax_socket_unpack(client)
    try:
        await client.connect(client.user_agent)
        await client._sync(client.user_agent)
        attachments = _build_pymax_attachments(media_files or [])
        last_message = None
        for index, chunk in enumerate(chunks):
            # Attach files to the first chunk only.
            chunk_attachments = attachments if index == 0 else None
            last_message = await client.send_message(
                text=chunk,
                chat_id=chat_id,
                notify=True,
                attachments=chunk_attachments or None,
                reply_to=reply_to if index == 0 else None,
            )
        return getattr(last_message, "id", None)
    finally:
        cleanup = getattr(client, "_cleanup_client", None)
        if cleanup:
            await cleanup()
        else:
            await client.close()


# ---------------------------------------------------------------------------
# MAX Adapter
# ---------------------------------------------------------------------------


class MaxAdapter(BasePlatformAdapter):
    """MAX user-account adapter (maxlib listener + pymax sender)."""

    def __init__(self, config, **kwargs):
        platform = Platform("max")
        super().__init__(config=config, platform=platform)

        extra = getattr(config, "extra", {}) or {}
        self._token, self._phone, self._profile_name = _resolve_credentials(extra)
        self.max_message_length = int(extra.get("max_message_length") or 4000)

        # Runtime state
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._incoming: Optional[asyncio.Queue] = None
        self._consumer_task: Optional[asyncio.Task] = None
        self._watchdog_task: Optional[asyncio.Task] = None
        self._listen_client: Any = None
        self._stopping = False
        self._send_lock = asyncio.Lock()
        self._me_id: Optional[int] = None
        self._seen_ids: deque = deque(maxlen=2000)

    @property
    def name(self) -> str:
        return "MAX"

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self._token:
            logger.error("MAX: no token — run `maxcli auth +7...` or set MAX_TOKEN")
            self._set_fatal_error(
                "config_missing",
                "MAX token not found (maxcli config or MAX_TOKEN)",
                retryable=False,
            )
            return False
        if not self._phone:
            logger.error("MAX: no phone — set MAX_PHONE or auth via maxcli")
            self._set_fatal_error(
                "config_missing",
                "MAX phone not found (maxcli config or MAX_PHONE)",
                retryable=False,
            )
            return False

        self._loop = asyncio.get_running_loop()
        self._incoming = asyncio.Queue()
        self._stopping = False

        try:
            self._listen_client = await asyncio.to_thread(self._start_listener)
        except Exception as e:
            logger.error("MAX: listener connect failed: %s", e)
            self._set_fatal_error("connect_failed", str(e), retryable=True)
            return False

        # Own user id for self-message filtering
        try:
            me = getattr(self._listen_client, "me", None)
            contact = getattr(me, "contact", me)
            self._me_id = getattr(contact, "id", None)
        except Exception:
            self._me_id = None
        if not self._me_id:
            logger.warning("MAX: could not resolve own user id — self-filter disabled")

        self._consumer_task = asyncio.create_task(self._consume_incoming())
        self._watchdog_task = asyncio.create_task(self._watchdog())

        self._mark_connected()
        logger.info(
            "MAX: connected as user %s (profile %s), listening for messages",
            self._me_id, self._profile_name,
        )
        return True

    async def disconnect(self) -> None:
        self._stopping = True
        self._mark_disconnected()

        for task in (self._consumer_task, self._watchdog_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._consumer_task = None
        self._watchdog_task = None

        client = self._listen_client
        self._listen_client = None
        if client is not None:
            try:
                await asyncio.to_thread(self._stop_listener, client)
            except Exception:
                pass

    # ── maxlib listener (blocking — runs in threads) ─────────────────────

    def _start_listener(self) -> Any:
        """Create and start a maxlib client. Blocking; call via to_thread."""
        maxlib = _load_maxlib()
        client = maxlib.MaxClient(token=self._token)

        def any_message(_client: Any, _message: Any) -> bool:
            return True

        @client.on_message(any_message)
        def on_message(_client: Any, message: Any) -> None:
            self._on_max_message(message)

        client.run()  # connects + spawns its WebMaxListener thread
        return client

    @staticmethod
    def _stop_listener(client: Any) -> None:
        try:
            client.stop()
        except Exception:
            pass
        try:
            client.disconnect()
        except Exception:
            pass

    def _on_max_message(self, message: Any) -> None:
        """Called from maxlib's listener thread — must not block or raise."""
        try:
            chat = getattr(message, "chat", None)
            chat_id = getattr(chat, "id", None) if chat is not None else None
            user = getattr(message, "user", None)
            contact = getattr(user, "contact", user)
            data = {
                "id": getattr(message, "id", None),
                "chat_id": chat_id,
                "sender": getattr(message, "sender", None),
                "sender_name": _contact_name(contact),
                "text": getattr(message, "text", None),
                "time": getattr(message, "time", None),
                "attaches": list(getattr(message, "attaches", None) or []),
            }
            if self._loop is not None and self._incoming is not None:
                self._loop.call_soon_threadsafe(self._incoming.put_nowait, data)
        except Exception:
            logger.exception("MAX: failed to enqueue incoming message")

    async def _watchdog(self) -> None:
        """Recreate the maxlib listener if its thread dies (no auto-reconnect)."""
        backoff = 5.0
        while not self._stopping:
            await asyncio.sleep(10)
            if self._stopping:
                break
            client = self._listen_client
            thread = getattr(client, "_t", None) if client is not None else None
            if client is not None and thread is not None and thread.is_alive():
                backoff = 5.0
                continue
            logger.warning("MAX: listener thread dead, reconnecting in %.0fs", backoff)
            await asyncio.sleep(backoff)
            if self._stopping:
                break
            try:
                if client is not None:
                    await asyncio.to_thread(self._stop_listener, client)
                self._listen_client = await asyncio.to_thread(self._start_listener)
                backoff = 5.0
                logger.info("MAX: listener reconnected")
            except Exception as e:
                logger.error("MAX: listener reconnect failed: %s", e)
                backoff = min(backoff * 2, 300)

    # ── Incoming dispatch ────────────────────────────────────────────────

    async def _consume_incoming(self) -> None:
        while True:
            data = await self._incoming.get()
            try:
                await self._process_incoming(data)
            except Exception:
                logger.exception("MAX: error processing incoming message")

    async def _process_incoming(self, data: dict) -> None:
        if not self._message_handler:
            return

        msg_id = data.get("id")
        sender = data.get("sender")
        chat_id = data.get("chat_id")
        if chat_id is None:
            return

        # Dedupe (reconnects may replay events)
        if msg_id is not None:
            key = (chat_id, msg_id)
            if key in self._seen_ids:
                return
            self._seen_ids.append(key)

        # Ignore our own messages
        if self._me_id is not None and sender is not None:
            try:
                if int(sender) == int(self._me_id):
                    return
            except (TypeError, ValueError):
                pass

        text = data.get("text") or ""
        attaches = data.get("attaches") or []
        if attaches:
            notes = " ".join(_describe_attachment(a) for a in attaches)
            text = f"{text}\n{notes}".strip() if text else notes
        if not text.strip():
            return

        # In MAX a DM chat_id equals me_id XOR sender_id
        chat_type = "dm"
        if self._me_id is not None and sender is not None:
            try:
                if (int(self._me_id) ^ int(sender)) != int(chat_id):
                    chat_type = "group"
            except (TypeError, ValueError):
                pass

        source = self.build_source(
            chat_id=str(chat_id),
            chat_name=data.get("sender_name") or str(chat_id),
            chat_type=chat_type,
            user_id=str(sender) if sender is not None else None,
            user_name=data.get("sender_name"),
        )

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            user_id=str(sender) if sender is not None else None,
            user_name=data.get("sender_name"),
            message_id=str(msg_id) if msg_id is not None else None,
            timestamp=_timestamp_to_dt(data.get("time")),
        )

        await self.handle_message(event)

    # ── Sending ───────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        if not self._token or not self._phone:
            return SendResult(success=False, error="MAX: not configured (token/phone)")
        try:
            target = int(chat_id)
        except (TypeError, ValueError):
            return SendResult(success=False, error=f"MAX: invalid chat_id {chat_id!r}")

        chunks = _split_text(content, self.max_message_length)
        reply_id = None
        if reply_to:
            try:
                reply_id = int(reply_to)
            except (TypeError, ValueError):
                pass

        async with self._send_lock:
            try:
                mid = await _pymax_send(
                    self._token, self._phone, target, chunks, reply_to=reply_id
                )
                return SendResult(
                    success=True,
                    message_id=str(mid) if mid is not None else None,
                )
            except Exception as e:
                logger.warning("MAX: send to %s failed: %s", chat_id, e)
                return SendResult(success=False, error=str(e), retryable=True)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Typing indicator not implemented for MAX — no-op."""
        pass

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": str(chat_id), "type": "dm"}


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------


def _bot():
    """Lazy import: bot mode must load even when maxlib/pymax are absent."""
    from . import bot_adapter

    return bot_adapter


def _resolve_mode(extra: Optional[dict] = None) -> str:
    """``bot`` or ``user``: explicit MAX_MODE / extra.mode wins, else a bot token implies bot."""
    extra = extra or {}
    mode = str(os.getenv("MAX_MODE") or extra.get("mode") or "").strip().lower()
    if mode in ("bot", "user"):
        return mode
    return "bot" if _bot().resolve_bot_token(extra) else "user"


def _user_deps_available() -> bool:
    try:
        import lz4.block  # noqa: F401
        import msgpack  # noqa: F401
        import maxlib  # noqa: F401
        from pymax import SocketMaxClient  # noqa: F401
    except ImportError:
        return False
    return True


def check_requirements() -> bool:
    """Passive probe: are the dependencies of the active transport importable?"""
    if _resolve_mode() == "bot":
        return _bot().HTTPX_AVAILABLE
    return _user_deps_available()


def _token_available() -> bool:
    if _bot().resolve_bot_token({}):
        return True
    token, _phone, _profile = _resolve_credentials({})
    return bool(token)


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    if _resolve_mode(extra) == "bot":
        return bool(_bot().resolve_bot_token(extra))
    token, _phone, _profile = _resolve_credentials(extra)
    return bool(token)


def is_connected(config) -> bool:
    return validate_config(config)


def _env_enablement() -> dict | None:
    """Seed PlatformConfig.extra from env/maxcli-config before adapter start.

    Returns None when no MAX credentials are resolvable (platform not configured).
    """
    if not _token_available():
        return None
    seed: dict = {}
    if _resolve_mode() == "user":
        phone = os.getenv("MAX_PHONE") or os.getenv("MAXCLI_PHONE")
        if phone:
            seed["phone"] = phone
        profile = os.getenv("MAX_PROFILE") or os.getenv("MAXCLI_PROFILE")
        if profile:
            seed["profile"] = profile
    home = os.getenv("MAX_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("MAX_HOME_CHANNEL_NAME", "MAX home"),
        }
    return seed


def _create_adapter(config):
    extra = getattr(config, "extra", {}) or {}
    if _resolve_mode(extra) == "bot":
        return _bot().MaxBotAdapter(config)
    return MaxAdapter(config)


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Out-of-process send for cron delivery (no live gateway adapter)."""
    extra = getattr(pconfig, "extra", {}) or {}
    if _resolve_mode(extra) == "bot":
        return await _bot().standalone_send(pconfig, chat_id, message, media_files=media_files)
    token, phone, _profile = _resolve_credentials(extra)
    if not token or not phone:
        return {
            "error": "MAX standalone send: no token/phone — "
                     "run `maxcli auth +7...` or set MAX_TOKEN and MAX_PHONE"
        }
    try:
        target = int(chat_id)
    except (TypeError, ValueError):
        return {"error": f"MAX standalone send: invalid chat_id {chat_id!r}"}

    chunks = _split_text(message, 4000)
    try:
        mid = await _pymax_send(
            token, phone, target, chunks, media_files=media_files
        )
        return {"success": True, "message_id": str(mid) if mid is not None else None}
    except Exception as e:
        logger.debug("MAX standalone send raised", exc_info=True)
        return {"error": f"MAX standalone send failed: {e}"}


_BOT_HINT = (
    "You are chatting via MAX messenger as a bot. MAX renders basic markdown "
    "(bold, italic, strikethrough, inline code, code blocks, links) but not "
    "tables or headings — keep formatting simple. Long messages are split "
    "into ~4000-character parts. Photos, voice messages and files the user "
    "sends are downloaded and available to you as local files."
)
_USER_HINT = (
    "You are chatting via MAX messenger (user account). MAX supports "
    "basic markdown but keep formatting simple. Long messages are "
    "automatically split into ~4000-character chunks. Attachments "
    "arrive as bracketed placeholders like [photo] or [file: name]."
)


def register(ctx):
    """Plugin entry point: called by the Hermes plugin system."""
    bot_mode = _resolve_mode() == "bot"
    ctx.register_platform(
        name="max",
        label="MAX",
        adapter_factory=_create_adapter,
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=[],
        install_hint=(
            "bot mode: set MAX_BOT_TOKEN (httpx only); user mode: "
            "pip install maxlib==0.1b1 maxapi-python==1.2.5 lz4 msgpack "
            "&& maxcli auth +7..."
        ),
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="MAX_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="MAX_ALLOWED_USERS",
        allow_all_env="MAX_ALLOW_ALL_USERS",
        max_message_length=4000,
        emoji="🔷",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=_BOT_HINT if bot_mode else _USER_HINT,
    )
