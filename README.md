# unicodex

An authenticated HTTP/SSE and WebSocket gateway for Codex. The proxy owns upstream
credentials, enforces per-user weekly allowances, and records reported usage in SQLite.
Clients authenticate with `Authorization: Bearer <gateway-key>`, configured
directly in Codex's `http_headers`. The key selects the local user; client OpenAI
tokens and account IDs do not establish the gateway identity. Unicodex does not
generate client credentials or modify client `auth.json` files.

The client setup below keeps `requires_openai_auth = true` and requires a Codex
patch for header precedence and account/status authentication. The proxy accepts
these headers; the existing sibling Codex patch supplies them on client requests.

Inbound and outbound enums retain concrete Tower services and futures. Each
outbound owns its associated observer. Cloned handles share connection pools and
credentials; only token refresh is serialized per upstream auth file. No boxed
service/future interfaces or application-wide request lock are used. `CodexInbound`,
`CodexOutbound`, their dispatch enums, and `App` are concrete types. Runtime
configuration strings and user identities use `Arc<str>`. The repository pins
nightly-2026-10-02 for `impl_trait_in_assoc_type`; Cargo selects it automatically.

## Server and client setup

Copy `config.example.yaml` to `config.yaml`, configure proxy-owned upstream
credentials, and set each inbound's gateway `key` directly in YAML:

```yaml
inbounds:
  - id: alice-route
    user: Alice
    type: codex
    key: "choose-a-local-secret"
    weekly_credits: "100"
```

Start the server:

```sh
cargo run -- --config config.yaml
```

The listener serves HTTP. Put Nginx TLS termination in front of it for Codex clients:
this Codex checkout requires an **HTTPS gateway origin** for ChatGPT workspace
routing. Forward HTTP and WebSocket upgrades to the configured listener.

After the Codex patch is available, merge these settings into the client's
`config.toml`. This example uses the local Nginx endpoint. Replace the header key
with the selected user's exact `key` from the server YAML; store it directly in
the configuration:

```toml
model_provider = "unicodex"
chatgpt_base_url = "https://localhost:8443/backend-api"

[model_providers.unicodex]
name = "unicodex"
base_url = "https://localhost:8443/backend-api/codex"
wire_api = "responses"
requires_openai_auth = true
supports_websockets = true
http_headers = { Authorization = "Bearer choose-a-local-secret" }
```

Leave `cli_auth_credentials_store` and the existing client `auth.json` unchanged.
There is no client-config generator or auth-file installation step. The model
URL handles inference; `chatgpt_base_url` selects the account/status backend.

Unmodified Codex adds stored authentication after provider headers and can
overwrite `Authorization`. Its account/status client also does not use provider
`http_headers`. Use the patched Codex build with this contract:

- The configured gateway `Authorization` takes precedence for HTTP model
  requests, WebSocket handshakes, and account/status requests.
- The gateway key authenticates requests; stored OpenAI tokens do not select the
  local user. Unicodex does not write client credentials. Codex's own login and
  token-refresh lifecycle remains unchanged.
- Account/status features remain available with `requires_openai_auth = true`.
  Cached account metadata cannot override the user selected by the gateway key.

After installing the patched client and updating its configuration, restart its
daemon so it picks up those settings. For an existing Alice client home:

```sh
env CODEX_HOME="$HOME/.local/share/unicodex/client-alice" \
  codex app-server daemon restart
env CODEX_HOME="$HOME/.local/share/unicodex/client-alice" codex
```

Keep the existing certificate trust configuration in the launch environment;
both processes must trust Nginx's certificate.

## Routing

Each outbound has `mode: broad` (default) or `mode: strict` for limited users.
Unlimited users always use broad forwarding:

- Broad permits unknown paths within configured upstream namespaces. Unprefixed
  paths retain the original `base_url + path + query` behavior.
- Strict accepts the concrete methods, paths, and transports in
  `codex_web_api_reference.md`; unknown combinations receive 404. Parameterized
  segments match one segment, and query parameters pass through unchanged.

| Gateway path | Destination |
|---|---|
| `/responses`, `/models`, other unprefixed paths | Model `base_url` |
| `/backend-api/codex/*` | Model `base_url`, stripping the gateway prefix |
| `/backend-api/*` | `chatgpt_base_url` |
| `/api/codex/*` | Product aliases mapped to `/wham/*` |
| `/v1/*` | `api_base_url` |
| `/auth/*`, `/oauth/*`, `/api/accounts/*`, `/codex/device` | `auth_base_url`, stripping `/auth` when present |

All accepted routes forward to their configured upstream. Only successful GET
account discovery (`/backend-api/wham/accounts/check`) and status
(`/backend-api/wham/usage`, including `/api/codex/usage`) responses are adapted.
The equivalent `/api/codex/accounts/check` alias uses the discovery adapter.
Profiles, reset-credit operations, history, analytics, auth, and remote-control
requests retain upstream response bodies. Requests without a recognized gateway
credential receive 404. Allowlisting a route does not implement its complete
product workflow.

The gateway pins destinations to configured bases. It does not intercept HTTPS
connections sent directly to OpenAI, follow presigned storage URLs, implement
OAuth login, or provide complete Realtime/remote-control session semantics.

## Upstream credential ownership

An `auth_file` accepts Codex-compatible `OPENAI_API_KEY` or a `tokens` object.
Refreshable files contain `access_token`, `refresh_token`, `id_token`, `account_id`
and `last_refresh`. Static access-token-only files and `token_env` remain supported.
Do not share a refreshable file with a separate Codex process or proxy instance.

The proxy refreshes before opening new upstream requests when token expiry is
within five minutes, falling back to eight days since `last_refresh` when expiry
cannot be read. Outbounds using the same canonical file share a refresh manager.
Updates preserve unreturned tokens and unrelated JSON fields, use a private atomic
file replacement, and become visible after persistence succeeds. If saving a
rotated token fails, the proxy retains it in memory and retries saving before
another refresh; forwarding stays blocked until saving succeeds.

A refresh has a 15-second timeout. Transient failures have a 30-second cooldown;
permanent failures and unusable successful token responses require replacing
credentials and restarting. On upstream 401,
the proxy refreshes for subsequent requests and returns 502 for the current one.
It does not replay a consumed request body. Existing WebSocket sessions continue.
Actual tokens are never returned to clients or printed in errors.

## Local credits and observations

The authenticated `user` (`name` is an alias) is the accounting identity; routing
IDs are configuration metadata. Gateway keys must be unique. Route selection is
final: authentication or admission failure never falls through to another user.

Each inbound must specify its only credit setting:

```yaml
weekly_credits: "100"        # Weekly allowance, using exact decimal credits
# or
weekly_credits: "unlimited"  # Upstream decides access; local usage is still logged
```

Remove `initial_credits` from existing configurations and set `weekly_credits`
explicitly for every user. Existing SQLite `credits` tables are left intact but
ignored; new databases contain only observations. Changing the allowance requires
a server restart and does not erase recorded consumption.

For limited users, admission compares the allowance with that user's reported
consumption in the upstream weekly window. The proxy fetches `/wham/usage` when
it needs current window boundaries and caches them per outbound/account until
reset. Status reads refresh or invalidate the cache; successful reset-credit
consumption invalidates it. Window discovery has a 30-second timeout. Account/status
and other metadata requests work at zero remaining credits. Inference is checked
before dispatch, before opening a WebSocket, and before each `response.create`.
Exhausted or zero allowances receive a Codex-compatible 429 `insufficient_quota`
error. Missing windows, unknown consumption, or database failures fail closed.
Running requests continue; concurrent requests can exceed the allowance because
there are no reservations or estimated token-to-credit conversions.

Unlimited users bypass local admission and always use broad forwarding, even on
a strict outbound. They receive upstream status, credit balances, quota windows,
and errors unchanged. Upstream account limits still apply. Usage is recorded
best-effort: accounting failures are logged and forwarding continues. The account
discovery compatibility adjustment described below still applies.

Account discovery preserves upstream account metadata and adapts the selected
workspace ID to the client's `ChatGPT-Account-ID`, so Codex can match its saved
login. If that header is absent or empty, the upstream ID remains. Map-shaped
account responses are normalized to Codex's list format. The selected workspace
origin is `NO_CONSTRAINT`, keeping requests on the configured HTTPS gateway.
Missing routing overrides receive `NO_CONSTRAINT`; existing overrides remain.
The gateway key alone determines the accounting user.

For limited users, status preserves upstream identity, plan, permission flags,
reset credits, and other metadata. Set an inbound's `weekly_credits` to a quoted nonnegative
decimal such as `"100"` to show that user's credits remaining in Codex's native
**Weekly limit** progress bar. The same allowance and observed consumption control
admission for that user.

The adapter keeps the upstream seven-day window (preferring the secondary window)
and changes only its `used_percent`. Consumption is the user's recorded
`usage_metadata.amount`, interpreted as credits, summed from
`reset_at - limit_window_seconds` inclusive to `reset_at` exclusive, excluding
future observations. Amounts use exact decimal arithmetic; the displayed percentage
is rounded and clamped to 0–100. The upstream `reset_at`, `reset_after_seconds`, and
window duration remain unchanged. Five-hour and additional-model windows, the
shared-account balance, and the monthly individual-limit row stay hidden.

Without an upstream weekly window, the local weekly bar is omitted. Missing or
invalid reported amounts also omit it instead of inventing a total. No recorded
usage means zero consumption; a zero allowance means 0% left.
Codex initially renders its cached snapshot while refreshing status.

`observations(timestamp, user, usage, credits)` stores only extracted accounting
fields. Usage and credit snapshots remain separate, with missing fields absent.
When recording succeeds, the observer persists the **original upstream report before delivery** and leaves
inference JSON, SSE, WebSocket messages, and quota headers unchanged. Status
reads record the original upstream credit/spend-control snapshot before adapting
presentation. Totals summarize reports; append-only observations are not
deduplicated into unique operations.

Inspectable JSON is buffered up to 16 MiB; SSE uses a fresh parser per response
and buffers individual events; WebSockets use complete messages. Unchanged body
bytes and trailers are preserved. Adapted account/status bodies invalidate stale body length
and integrity headers. For limited users, JSON observation errors surface before
response delivery; SSE/body errors abort streaming and WebSocket observation
errors close with 1011. Identity encoding is requested; compressed accounting
responses are rejected for limited users. For unlimited users, recording errors
are non-blocking, and unsupported encodings or observation limits disable body
inspection while preserving buffered bytes, the remaining stream, and trailers.

## Inspect real ChatGPT responses

Use `scripts/chatgpt-request.sh` to query ChatGPT directly with your existing
Codex ChatGPT login. It uses `jq` to read `tokens.access_token` and
`tokens.account_id` from `${CODEX_HOME:-$HOME/.codex}/auth.json`:

```sh
./scripts/chatgpt-request.sh                     # Account check
./scripts/chatgpt-request.sh usage > usage.json
./scripts/chatgpt-request.sh profile
./scripts/chatgpt-request.sh '/backend-api/wham/usage/plan_limit_history?days=7'
./scripts/chatgpt-request.sh --auth /path/to/auth.json account/check
```

The default endpoint is `https://chatgpt.com/backend-api/wham/accounts/check`.
JSON is pretty-printed to stdout; HTTP status goes to stderr. `--raw` preserves
response bytes, including non-JSON error bodies. Use `-d @request.json` for a JSON
POST or `-X METHOD` to choose a method. Responses are buffered with a 120-second
timeout. HTTP errors retain their body and produce a nonzero exit status.

The script reads credentials without changing or refreshing them and keeps auth
headers out of curl's arguments. A 401 requires a valid refreshed ChatGPT login.
It requires file-based ChatGPT credentials; an API-key-only login or credentials
stored only in the OS keyring cannot supply these fields. See the
[official authentication documentation](https://learn.chatgpt.com/docs/auth#credential-storage).

## Logging

Unicodex writes timestamped, readable text logs to stderr. The default filter is
`warn,unicodex=info`: startup/shutdown and request summaries are visible, while
dependency logs are limited to warnings and errors. Control verbosity with
`RUST_LOG`, for example:

```sh
RUST_LOG=warn,unicodex=debug cargo run -- --config config.yaml
```

An unset or empty filter uses the default. An invalid filter produces a warning
and falls back to the default. Redirected output contains no color escapes.
Use your service manager or container runtime to capture and rotate stderr.

Each request has an internal `request_id`, method, path, and the authenticated
user and inbound/outbound IDs when available. `response ready` includes status
and `response_ready_ms`, which measures time until response construction finishes,
not the lifetime of an SSE stream or WebSocket session. Successes log at INFO,
4xx responses at WARN, and 5xx responses at ERROR. DEBUG adds route selection,
upstream status/timing, and stream/session lifecycle details. A routing rejection
distinguishes `no_matching_inbound` (including an absent or incorrect gateway key)
from `unsupported_route` (such as an unstripped URL prefix in strict mode).

Late response-body and WebSocket failures retain the request context. Credential
refresh logs describe success, failure, and whether another attempt is possible;
database and credential-persistence failures identify the operation. Application
logs omit headers, query strings, credentials, payloads, and raw error chains,
including at DEBUG. Enabling verbose third-party dependency logs is separate from
the application's logging policy.

## Verification

Tests live beside implementations; there are no `mod.rs` or separate test files.

```sh
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

Tests cover YAML keys selecting the correct user even with a missing or spoofed
client account ID, HTTP/WebSocket upstream credential replacement, credit checks,
streaming observation, persistence failures, and proxy-owned token refresh. All
verification uses synthetic credentials and no real upstream account.

The sibling Codex build was also verified against an isolated HTTPS gateway with
synthetic client credentials and a mock upstream. Workspace discovery matched the
saved client account ID, account rate-limit parsing preserved decimal amounts,
and inference reports were recorded. The temporary client's auth file contents
and modification time stayed unchanged. No real upstream account or existing
client home was used. That verification predates the weekly status display change.
