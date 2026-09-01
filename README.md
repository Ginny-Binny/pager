# pager

A small personal uptime monitor. It polls HTTP status endpoints, keeps every
piece of state in Redis, and pages your phone through a self-hosted
[ntfy](https://ntfy.sh) server — with consecutive-failure thresholds, dedupe,
acknowledgement and escalation.

**All state lives in Redis, never in process memory.** A restart never loses a
pending escalation and never re-pages an incident you already acked. The three
decisions that must not race — opening an incident, claiming an escalation,
acking — are each a single Lua script, so the test and the write happen
together rather than as two round trips with a gap in between.

Standard library plus `go-redis` and a YAML parser. No web framework.

---

## How it behaves

| Event | What happens |
|---|---|
| 3 consecutive failures | Incident opens, priority-5 page with the failure reason, latency and an ack link |
| Still failing | Nothing. The incident already exists, so there is no second page |
| Unacked for 5 min | Escalates to level 2 and re-pages. A voice-call stub sits at this level |
| Unacked for 10 min | Escalates to level 3 and re-pages |
| Beyond level 3 | Goes quiet. The incident stays open in `/status`, and the poller keeps its TTL alive so it can never expire and resurface as a fresh page tomorrow |
| You tap Ack | Escalation stops immediately. You still get the recovery message |
| 2 consecutive successes | Incident resolves, priority-2 recovery message with the downtime, incident deleted |

Thresholds are one-directional: only a healthy check can fall down, and only a
down check can come back up, so a flap inside a threshold changes nothing.

---

## Setup

### 1. DNS

Point two names at the host, both as A/AAAA records:

```
ntfy.example.com   -> your.server.ip
pager.example.com  -> your.server.ip
```

Caddy needs ports 80 and 443 reachable to issue certificates. Do this before
the first start, or certificate issuance fails and you will be debugging TLS
instead of the monitor.

### 2. Configure

```bash
cp .env.example .env
$EDITOR .env          # set the two domains, ACME_EMAIL, PUBLIC_BASE_URL, NTFY_TOPIC
```

Leave `NTFY_TOKEN` as the placeholder for now — it does not exist yet.

Generate the `/status` password hash and put it in `.env`:

```bash
docker run --rm caddy:2-alpine caddy hash-password
```

### 3. Create the ntfy user and token

Start ntfy on its own first, because the token has to exist before the monitor
will boot:

```bash
docker compose up -d ntfy
```

The server runs with `auth-default-access: deny-all`, so nothing can read or
write any topic until you grant it. Create one user with read and write on your
topic — it publishes from the monitor and subscribes from your phone:

```bash
docker compose exec ntfy ntfy user add --role=user pager
# prompts for a password; you will type this into the Android app

docker compose exec ntfy ntfy access pager alerts rw    # use your NTFY_TOPIC
docker compose exec ntfy ntfy token add pager
```

The last command prints a token like `tk_a1b2c3…`. Put it in `.env` as
`NTFY_TOKEN`.

> Splitting this into two users — one write-only for the monitor, one read-only
> for the phone — is stricter and worth doing if the token ever leaves your
> devices:
> ```bash
> docker compose exec ntfy ntfy access monitor alerts write-only
> docker compose exec ntfy ntfy access phone   alerts read-only
> ```

Verify access came out right:

```bash
docker compose exec ntfy ntfy access pager
```

### 4. Start everything

```bash
docker compose up -d
docker compose logs -f pager
```

You should see a `starting pager` line listing the checks, then a poll cycle.

```bash
curl -fsS https://pager.example.com/health
curl -fsS -u admin https://pager.example.com/status | jq
```

`/health` returns 200 with the timestamp of the last completed poll cycle, and
503 if that cycle is older than three intervals or Redis is unreachable. Point
an external heartbeat service at it — that is the only thing that can tell you
the monitor itself has stopped.

---

## Configuring the ntfy Android app

1. Install ntfy — [Play Store](https://play.google.com/store/apps/details?id=io.heckel.ntfy)
   or [F-Droid](https://f-droid.org/packages/io.heckel.ntfy/).
2. **Settings → General → Default server**: `https://ntfy.example.com`
3. **Settings → General → Manage users → Add user**: the `pager` username and
   password from step 3 above.
4. Back on the main screen, **+** to subscribe, enter your topic name
   (`alerts`), and make sure "Use another server" points at your server.
5. Long-press the subscription → **Settings**:
   - **Notification priority → Max** — pages arrive at priority 5, and only
     Max gets a full alarm sound.
   - Enable **Override Do Not Disturb** so a 3am outage actually wakes you.
     Android may send you into system settings to grant the app a DND
     exception; do that.
6. **F-Droid build only**: Settings → **Instant delivery** → on, for the
   subscription. The F-Droid build has no Firebase, so without this it polls
   every ~15 minutes and a page can arrive quarter of an hour late. Instant
   delivery holds a foreground connection and adds a persistent notification —
   that is the trade, and it is the right one for a pager.
7. Also whitelist ntfy from battery optimisation
   (Android Settings → Apps → ntfy → Battery → Unrestricted). Doze mode will
   otherwise delay notifications no matter what the app is set to.

Send yourself a test page:

```bash
curl -H "Authorization: Bearer $NTFY_TOKEN" \
     -H "Title: test page" -H "Priority: 5" -H "Tags: rotating_light" \
     -d "if this buzzes, the phone side is done" \
     https://ntfy.example.com/alerts
```

---

## Testing it end to end

Point a check at a URL that cannot work, and watch the whole ladder run:

```bash
# in checks.yaml
  - name: connect-app
    url: https://connect.pabbly.com/api/this-endpoint-does-not-exist
```

```bash
docker compose restart pager
docker compose logs -f pager
```

With the defaults you should see:

| Time | What |
|---|---|
| ~0–2 min | Two `probe failed` log lines, no page |
| ~3 min | `state transition: UP -> DOWN`, then a page on your phone |
| ~4–7 min | More `probe failed` lines, still no page — dedupe working |
| ~8 min | `[L2] … is STILL DOWN`, plus the phone-call stub log line |
| ~13 min | `[L3] … is STILL DOWN` |
| after | Silence. `/status` still shows it firing at level 3 |

Tap **Ack** in the notification: you get an "Acknowledged" page and escalation
stops. Then put the correct URL back and `docker compose restart pager` — after
two successful cycles you get a priority-2 recovery message with the downtime.

To confirm restart safety, restart mid-incident while a check is still failing.
You should get no duplicate page, and `/status` should show the same
`first_seen` and `escalation_level` as before.

---

## Adding a check

Edit `checks.yaml` and restart — the file is mounted read-only into the
container, so no rebuild is needed:

```yaml
  - name: my-service          # [A-Za-z0-9_-], becomes a Redis key and URL segment
    url: https://example.com/health
```

```bash
docker compose restart pager
```

Optional per-check fields, all unset by default (so healthy means "HTTP 200
within 5s"):

| Field | Effect |
|---|---|
| `expected_status` | Status code that counts as healthy (default 200) |
| `body_contains` | Substring that must appear in the body |
| `json_path` + `expected_value` | Dot path into the JSON response and the value it must equal |
| `max_latency_ms` | Fail even on a 200 if slower than this |
| `timeout_ms` | Per-probe timeout (default 5000) |
| `follow_redirects` | Default false — a redirect on a status endpoint is a misconfiguration worth seeing, not chasing to someone else's 200 |

`json_path` is a **dot path**, not full JSONPath: object keys and numeric array
indices only, e.g. `status`, `data.db.ok`, `items.0.name`. No wildcards,
filters or recursive descent — that is what keeps the dependency list to two
libraries.

Config is validated at startup and the process refuses to boot on a bad file,
including unknown fields: a typo like `body_contain` would otherwise be
silently ignored and quietly weaken the check.

---

## Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | 200 with the last completed cycle; 503 if stale, never run, or Redis is unreachable. Open, for external heartbeat monitoring |
| `GET /status` | JSON of every check and its incident. Behind basic auth in Caddy — it lists internal names and URLs |
| `GET\|POST /ack/{check}` | Acknowledge. Needs the per-incident token, as `?t=` or an `X-Ack-Token` header |

### Why the ack token exists

Check names are guessable and link-preview bots follow URLs, so a plain
`GET /ack/{check}` could be silenced by something merely *scraping* the
notification. Instead each incident carries 128 bits of `crypto/rand`, stored
in its Redis hash:

- The link in the body carries the token in `?t=`, compared in constant time.
- The **Ack** button in the notification uses an HTTP **POST** with the token
  in a header, so a prefetched GET cannot ack anything.
- The token dies with the incident, so a stale link from last week's outage
  cannot silence today's.
- `/status` never serialises it.

---

## Configuration reference

| Variable | Default | Meaning |
|---|---|---|
| `NTFY_URL` | *required* | ntfy base URL, internal (`http://ntfy:80`) in Compose |
| `NTFY_TOPIC` | *required* | Topic to publish to |
| `NTFY_TOKEN` | — | Bearer token; omit only on an unauthenticated server |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `PUBLIC_BASE_URL` | `http://localhost:8080` | Public URL, used to build ack links |
| `LISTEN_ADDR` | `:8080` | HTTP listen address |
| `POLL_INTERVAL` | `60s` | Time between poll cycles |
| `FAILURE_THRESHOLD` | `3` | Consecutive failures before DOWN |
| `RECOVERY_THRESHOLD` | `2` | Consecutive successes before UP |
| `ESCALATION_INTERVAL` | `5m` | Silence before escalating |
| `CHECKS_FILE` | `checks.yaml` | Path to the checks file |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

`NTFY_URL` and `NTFY_TOPIC` are required rather than defaulted: a monitor that
starts up and pages nobody is worse than one that refuses to start.

---

## Development

```bash
go test ./...
go test -race ./...
go vet ./...
```

Validating the Caddyfile needs the environment populated — it reads
`{$ACME_EMAIL}` and expands it in place, so a bare `email` directive is a
parse error and validating with an empty environment fails on a config that
is actually fine:

```bash
docker run --rm --env-file .env \
  -v "$PWD/Caddyfile:/etc/caddy/Caddyfile:ro" \
  caddy:2-alpine caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
```

Tests use [miniredis](https://github.com/alicebob/miniredis), including for the
Lua scripts, and inject a clock — so escalation timing is verified
deterministically without any `time.Sleep`. Covered: threshold and dedupe
behaviour, restart safety, the escalation ladder and its cap, TTL survival
across a simulated day, ack token handling, `/health` staleness detection, and
that `/status` does not leak the ack token.

Run locally against a real Redis:

```bash
docker run -d -p 6379:6379 redis:7-alpine
NTFY_URL=https://ntfy.example.com NTFY_TOPIC=alerts NTFY_TOKEN=tk_… \
PUBLIC_BASE_URL=http://localhost:8080 go run .
```

---

## Known limits

- **Redis is a single point of failure.** If it is down the monitor cannot
  record or decide anything. It logs loudly and `/health` returns 503 rather
  than pretending everything is fine.
- **It cannot report on its own host.** If this box dies, nothing pages. That
  is what the external heartbeat on `/health` is for.
- **The phone-call escalation at level 2 is a stub.** It logs where a Twilio or
  Exotel call would go and does not place one — see
  `internal/engine/engine.go`. A half-wired call path that fails silently is
  worse than none.
- **No TLS certificate expiry warning** on probed endpoints. An expired cert
  shows up as a probe failure on the day it expires, not before.
