# hermes-max

MAX messenger platform adapter (gateway plugin) for [Hermes Agent](https://github.com/NousResearch/hermes-agent).

Connects a Hermes gateway to [MAX](https://max.ru) using a **user account**
(second phone number) instead of a commercial bot — anyone can run it with
their own number.

The transport is vendored from [maxcli](https://github.com/dapi/maxcli):

- **Incoming messages** — `maxlib` WebSocket listener (with the local
  `Origin`-header handshake patch), reconnecting watchdog included.
- **Outgoing messages** — `maxapi-python` (pymax) ephemeral client per send,
  serialized by a lock; long texts are split into ~4000-char chunks.
- **Credentials** — shared with maxcli token storage
  (`~/.config/maxcli/config.json`): authenticate once, both tools reuse
  the session.

## Install

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

## Configuration

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

## Caveats

- Uses the **unofficial** MAX internal WebSocket API (user account). It may
  break without notice and may violate MAX ToS — like any user-account bridge.
- The `maxlib` listener is supervised by a reconnecting watchdog inside the
  adapter; the pymax sender reconnects per send.

## License

GPL-3.0 (contains vendored code from [maxcli](https://github.com/dapi/maxcli)).
