# hermes-max

MAX messenger platform adapter (gateway plugin) for [Hermes Agent](https://github.com/NousResearch/hermes-agent).

Connects a Hermes gateway to [MAX](https://max.ru) in one of two modes:

| Mode | Account | Transport | Dependencies |
|---|---|---|---|
| **bot / polling** | a MAX bot | long polling `GET /updates`, `POST /messages` | httpx (ships with Hermes) |
| **bot / webhook** | a MAX bot | HTTPS webhook, `POST /messages` | httpx + aiohttp (ship with Hermes) |
| **user** | a user account (second phone number) | unofficial WebSocket API via vendored [maxcli](https://github.com/dapi/maxcli) transport | maxlib, maxapi-python, lz4, msgpack |

Bot mode is selected when `MAX_BOT_TOKEN` is set (or `extra.mode: bot`);
otherwise the adapter runs in user mode. `MAX_MODE=bot|user` forces a mode.

## Bot mode

```bash
# 1. Install the plugin
hermes plugins install dapi/hermes-max
hermes plugins enable max-platform

# 2. Give the gateway the bot token (from the MAX bot settings)
export MAX_BOT_TOKEN=...            # keep it in your secret store, not in git
export MAX_ALLOWED_USERS=<your MAX user id>

# 3. Enable the platform in ~/.hermes/config.yaml
#    platforms:
#      max:
#        enabled: true
#        extra:
#          mode: bot

# 4. Restart the gateway, then open the bot in MAX and press Start
hermes gateway restart
```

What works: text both ways (markdown, long answers split into 4000-char
parts, message edits), typing indicator, replies and forwards as context,
inbound photos / voice / video / files (downloaded into the Hermes media
cache with bounded retries for temporary CDN timeouts and 5xx/429 responses),
outbound images / documents / video / audio via `POST /uploads`, cron delivery
through `MAX_HOME_CHANNEL`.

By default the adapter uses long polling. Set `MAX_WEBHOOK_URL` and
`MAX_WEBHOOK_SECRET` to switch bot mode to webhook delivery. The adapter binds
a local HTTP listener, verifies `X-Max-Bot-Api-Secret`, registers the public
URL with `POST /subscriptions`, and does not start `GET /updates`.

```bash
export MAX_WEBHOOK_URL=https://max-hooks.office.brandymint.ru/max/webhook
export MAX_WEBHOOK_SECRET=...       # random secret, keep outside git
export MAX_WEBHOOK_HOST=0.0.0.0     # optional, default
export MAX_WEBHOOK_PORT=8645        # optional, default
export MAX_WEBHOOK_PATH=/max/webhook
```

Webhook mode is preferred for production and for native microphone voice
messages: current MAX clients may produce incomplete events over long polling,
while webhook delivery includes the `audio` attachment URL.

Long-poll mode needs no public endpoint: the last acknowledged marker is kept
in `$HERMES_HOME/max_bot_state.json`, so a restart does not replay messages.
Unknown users get the standard Hermes pairing prompt unless listed in
`MAX_ALLOWED_USERS`.

| Env var | Description |
|---|---|
| `MAX_BOT_TOKEN` | Bot token (required for bot mode) |
| `MAX_API_BASE_URL` | API base, default `https://platform-api2.max.ru` |
| `MAX_CA_BUNDLE` | PEM bundle for hosts signed by the Russian Trusted CA |
| `MAX_WEBHOOK_URL` | Public HTTPS callback URL; enables webhook mode when set |
| `MAX_WEBHOOK_SECRET` | Secret expected in `X-Max-Bot-Api-Secret` (required in webhook mode) |
| `MAX_WEBHOOK_HOST` | Local listener bind host, default `0.0.0.0` |
| `MAX_WEBHOOK_PORT` | Local listener port, default `8645` |
| `MAX_WEBHOOK_PATH` | Local callback path, default `/max/webhook` |
| `MAX_MODE` | Force `bot` or `user` |
| `MAX_HOME_CHANNEL` | Chat id for cron / notification delivery |
| `MAX_ALLOWED_USERS` | Comma-separated MAX user ids allowed to talk to the agent |
| `MAX_ALLOW_ALL_USERS` | `true` to allow anyone (dev only) |

## User-account mode

Uses a **user account** instead of a bot — anyone can run it with their own
number. The transport is vendored from [maxcli](https://github.com/dapi/maxcli):

- **Incoming messages** — `maxlib` WebSocket listener (with the local
  `Origin`-header handshake patch), reconnecting watchdog included.
- **Outgoing messages** — `maxapi-python` (pymax) ephemeral client per send,
  serialized by a lock; long texts are split into ~4000-char chunks.
- **Credentials** — shared with maxcli token storage
  (`~/.config/maxcli/config.json`): authenticate once, both tools reuse
  the session.

### Install

```bash
# 1. Install the plugin
hermes plugins install dapi/hermes-max
hermes plugins enable max-platform

# 2. Install transport dependencies into the hermes venv
uv pip install --python ~/.hermes/venvs/hermes-mise-3.13/bin/python \
    maxlib==0.1b1 maxapi-python==1.2.5 lz4 msgpack

# 3. Authenticate the MAX account (interactive, SMS code)
maxcli auth +7XXXXXXXXXX

# 4. Enable the platform in ~/.hermes/config.yaml
#    platforms:
#      max:
#        enabled: true

# 5. Restart the gateway
hermes gateway restart
```

Or clone manually into the plugins directory:

```bash
git clone https://github.com/dapi/hermes-max ~/.hermes/plugins/max
```

### Configuration

All settings are optional — by default the adapter reads token/phone from the
maxcli config profile.

| Env var | Description |
|---|---|
| `MAX_TOKEN` / `MAXCLI_TOKEN` | Session token (default: `~/.config/maxcli/config.json`) |
| `MAX_PHONE` / `MAXCLI_PHONE` | Account phone, e.g. `+79990000000` |
| `MAX_PROFILE` / `MAXCLI_PROFILE` | maxcli config profile (default: `default`) |
| `MAXCLI_CONFIG_DIR` | Override maxcli config directory |
| `MAX_HOME_CHANNEL` | Chat id for cron / notification delivery |
| `MAX_ALLOWED_USERS` | Comma-separated MAX user ids allowed to talk to the agent |
| `MAX_ALLOW_ALL_USERS` | `true` to allow anyone (dev only) |

### Multiple MAX accounts

maxcli supports named profiles, so your personal maxcli usage and the gateway
can live on different phone numbers:

```bash
# Personal account stays on the default profile
maxcli auth +79033891228

# Gateway account (second number) goes to a dedicated profile
maxcli auth --profile hermes +79033892417
```

Then point the adapter at that profile in `~/.hermes/config.yaml`:

```yaml
platforms:
  max:
    enabled: true
    extra:
      profile: hermes
```

(or set `MAX_PROFILE=hermes` in the gateway environment).

## Caveats

- User mode uses the **unofficial** MAX internal WebSocket API. It may
  break without notice and may violate MAX ToS — like any user-account bridge.
  Bot mode uses only the official Bot API.
- The `maxlib` listener is supervised by a reconnecting watchdog inside the
  adapter; the pymax sender reconnects per send.

## Tests

Bot mode is covered by tests against a mocked Bot API (no network):

```bash
PYTHONPATH=~/.hermes/hermes-agent python -m pytest tests/
```

## License

GPL-3.0 (contains vendored code from [maxcli](https://github.com/dapi/maxcli)).
