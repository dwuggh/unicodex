# Codex Web API Reference

Current architecture reference for `openai/codex` (2026-09-30).

### Gateway implementation scope

Unicodex defaults to broad forwarding; per-outbound `mode: strict` restricts limited users' requests
to the concrete method/path/transport combinations below. Recognizing a route does
not implement its complete product workflow. Accepted routes forward upstream;
only successful account-discovery and status GET responses (including their
`/api/codex` aliases) are adapted for workspace compatibility and local weekly
credits remaining. Unlimited users always use broad forwarding and retain upstream
status responses, with best-effort usage recording. Profile, reset-credit, auth, history, analytics, and remote-control
responses pass through.

Clients authenticate with a configured gateway HTTP header and keep their existing
`auth.json`. The proxy separately owns and refreshes upstream credentials.
Inference accounting reports are persisted before delivery without changing their
content or quota headers. See [README.md](README.md) for configuration, weekly
status accounting, transport limits, and verification.

## 1. Base URL notation

| Symbol | Base URL | Meaning |
|---|---|---|
| **A** | `https://api.openai.com/v1` | Normal OpenAI API/provider endpoint, typically API-key mode |
| **B** | `https://chatgpt.com/backend-api` | General ChatGPT product/account backend |
| **C** | `https://chatgpt.com/backend-api/codex` | Codex model/inference backend used by ChatGPT-login mode |
| **AUTH** | `https://auth.openai.com` | OAuth/device/PAT/agent-identity authentication service |

For ChatGPT-account authentication, model traffic defaults to **C** while account/product traffic uses **B**. A configured model-provider `base_url` replaces the model/inference endpoint (C) but does not automatically replace `chatgpt_base_url` (B).

---

## 2. Core model/provider APIs

| Purpose | Method / URL pattern | Main params / body | Auth / important headers | Proxy relevance | Source map |
|---|---|---|---|---|---|
| Responses HTTP/SSE | `POST {A or C}/responses` | `model`, `instructions`, `input`, `tools`, `tool_choice`, `parallel_tool_calls`, `reasoning`, `store`, `stream`, `stream_options`, `include`, `service_tier`, `prompt_cache_key`, `text`, `client_metadata`, `access_programs` | Provider auth; Codex routing/session headers | **Critical** | `codex-rs/codex-api/src/endpoint/responses.rs`; `codex-rs/codex-api/src/common.rs`; `codex-rs/core/src/client.rs` |
| Responses WebSocket | `WSS {A or C}/responses` | WS `response.create`; mostly same fields as HTTP plus `previous_response_id`, `generate` | Provider auth; `OpenAI-Beta: responses_websockets=2026-02-06`; Codex metadata | **Critical for max parity** | `codex-rs/codex-api/src/endpoint/responses_websocket.rs`; `codex-rs/core/src/client.rs` |
| Model catalog | `GET {A or C}/models?client_version=...` | `client_version`; provider query params; ETag | Provider auth | Recommended | `codex-rs/codex-api/src/endpoint/models.rs`; `codex-rs/model-provider/src/models_endpoint.rs` |
| Standalone web search | `POST {A or C}/alpha/search` | `id`, `model`, `reasoning`, `input`, `commands`, `settings`, `max_output_tokens` | Provider auth | Recommended if standalone web search enabled | `codex-rs/codex-api/src/endpoint/search.rs`; `codex-rs/codex-api/src/search.rs` |
| Memory summarization | `POST {A or C}/memories/trace_summarize` | `model`, `traces`, optional `reasoning` | Provider auth; subagent/memory headers | Optional | `codex-rs/codex-api/src/endpoint/memories.rs`; `codex-rs/core/src/client.rs` |
| Image generation | `POST {A or C}/images/generations` | `prompt`, `model`, `background`, `n`, `quality`, `size` | Provider auth | Optional | `codex-rs/codex-api/src/endpoint/images.rs`; `codex-rs/codex-api/src/images.rs` |
| Image edit | `POST {A or C}/images/edits` | `images[]`, `prompt`, `model`, `background`, `n`, `quality`, `size` | Provider auth | Optional | same as above |
| Realtime/WebRTC call | `POST {A or C}/realtime/calls` | SDP; optionally session config; AVAS may add `intent=quicksilver&architecture=avas` | Provider auth | Optional | `codex-rs/codex-api/src/endpoint/realtime_call.rs` |
| Frameless Realtime call | `POST {A}/live` | multipart `sdp` + `session` | Provider auth | Optional | `codex-rs/codex-api/src/endpoint/realtime_call.rs` |
| Realtime WebSocket | `WSS .../realtime` or `WSS .../live` | Realtime session/audio/event frames | Provider auth; session headers | Optional | `codex-rs/codex-api/src/endpoint/realtime_websocket/*` |

### Responses-specific headers that should be preserved

Typical current Codex headers include:

- `originator`
- `version`
- `OpenAI-Organization`
- `OpenAI-Project`
- `session-id`
- `thread-id`
- `x-client-request-id`
- `x-codex-installation-id`
- `x-codex-routing-hint`
- `x-codex-turn-state`
- `x-codex-turn-metadata`
- `x-codex-parent-thread-id`
- `x-codex-window-id`
- `x-codex-beta-features`
- `x-openai-subagent`
- `x-openai-memgen-request`
- `x-responsesapi-include-timing-metrics`
- `OpenAI-Beta`
- `x-openai-internal-codex-responses-lite`
- optional attestation headers

For a transparent model gateway, replace only gateway-controlled transport/auth fields such as `Authorization` and `Host`; preserve unknown request/response fields and SSE/WS payloads.

---

## 3. Authentication on model requests

| Auth mode | Main request auth |
|---|---|
| OpenAI API key / provider token | `Authorization: Bearer <token>` |
| ChatGPT account | `Authorization: Bearer <ChatGPT access token>` + `ChatGPT-Account-ID: <account/workspace>` |
| FedRAMP ChatGPT account | above + `X-OpenAI-Fedramp: true` |
| Agent identity | `Authorization: AgentAssertion <signed assertion>` + account metadata |
| Custom provider with `requires_openai_auth = false` | no ambient OpenAI auth unless explicitly configured |

Source map: `codex-rs/model-provider/src/auth.rs`, `bearer_auth_provider.rs`, `combined_auth.rs`.

---

## 4. ChatGPT account/product backend APIs (B)

Production B base: `https://chatgpt.com/backend-api`.

The backend client supports two path styles:

- ChatGPT: `/wham/...`
- alternate Codex API host: `/api/codex/...`

| Purpose | ChatGPT route | Method / params | Auth / headers | Source map |
|---|---|---|---|---|
| Account check | `GET B/wham/accounts/check` | none | ChatGPT auth | `codex-rs/backend-client/src/client.rs` |
| Profile | `GET B/wham/profiles/me` | none | ChatGPT auth | same |
| Add-credit nudge | `POST B/wham/accounts/send_add_credits_nudge_email` | JSON `credit_type` | ChatGPT auth | same |
| Task list | `GET B/wham/tasks/list` | `limit`, `task_filter`, `cursor`, `environment_id` | ChatGPT auth | same |
| Task detail | `GET B/wham/tasks/{task_id}` | path param | ChatGPT auth | same |
| Sibling turns | `GET B/wham/tasks/{task}/turns/{turn}/sibling_turns` | path params | ChatGPT auth | same |
| Create cloud task | `POST B/wham/tasks` | JSON task body | ChatGPT auth | same |
| Managed config | `GET B/wham/config/bundle` | none | ChatGPT auth | same |
| User settings | `GET B/wham/settings/user` | none | ChatGPT auth; `Cache-Control: no-cache, no-store` | same |
| Workspace messages | `GET B/wham/workspace-messages` | none | ChatGPT auth; `Cache-Control: no-store` | same |
| Usage/rate limits | `GET B/wham/usage` | none | ChatGPT auth | `backend-client/src/client/rate_limit_resets.rs` |
| Reset-credit inventory | `GET B/wham/rate-limit-reset-credits` | none | ChatGPT auth | same |
| Consume reset credit | `POST B/wham/rate-limit-reset-credits/consume` | reset-credit request | ChatGPT auth | same |
| Plan limit history | `GET B/wham/usage/plan_limit_history?days=7` | `days=7` | ChatGPT auth | `backend-client/src/client/plan_history.rs` |
| Turn-cost estimate | `POST B/wham/usage/thread-estimates/query` | query body | ChatGPT auth | `backend-client/src/client/chatgpt_turn_cost.rs` |
| Task usage | `POST B/wham/usage/thread_usage/query_v2` | query body | ChatGPT auth | `backend-client/src/client/task_usage.rs` |
| Thread usage | `POST B/wham/usage/thread_usage/query` | query body | ChatGPT auth | `backend-client/src/client/thread_usage.rs` |

### Analytics routes

- `B/wham/usage/daily-token-usage-breakdown`
- `B/wham/usage/credit-usage-events`
- `B/wham/usage/daily-workspace-user-token-usage-breakdown`
- `B/wham/usage/daily-workspace-user-credit-usage`
- `B/wham/analytics/daily-workspace-usage-counts`
- `B/wham/analytics/daily-plugin-usage-metrics`
- `B/wham/analytics/daily-skill-usage-metrics`

Source: `codex-rs/backend-client/src/client/analytics.rs`.

API-key accounting also has `POST https://api.openai.com/v1/analytics/codex/turn-costs` in `backend-client/src/client/turn_usage.rs`.

---

## 5. Cloud-task environment APIs

| Purpose | ChatGPT route | Alternate route | Method | Main params | Source |
|---|---|---|---|---|---|
| List environments | `B/wham/environments` | `/api/codex/environments` | GET | none | `codex-rs/cloud-tasks/src/env_detect.rs` |
| Environments by repo | `B/wham/environments/by-repo/github/{owner}/{repo}` | `/api/codex/environments/by-repo/github/{owner}/{repo}` | GET | owner/repo path params | same |

Cloud-task CRUD then reuses the `/wham/tasks...` backend-client routes.

---

## 6. Connector / Apps APIs

| Purpose | Route | Method / params | Auth / headers | Source |
|---|---|---|---|---|
| Public connector directory | `GET B/connectors/directory/list?external_logos=true` | optional pagination `token` | ChatGPT auth | `codex-rs/connectors/src/lib.rs`; `codex-rs/chatgpt/src/connectors.rs` |
| Workspace connector directory | `GET B/connectors/directory/list_workspace?external_logos=true` | none | ChatGPT auth | same |
| Apps batch metadata | `POST B/ps/apps/batch` | `{app_ids:[...], include_tools:bool}` | ChatGPT auth; `OAI-Product-Sku: codex` | `codex-rs/chatgpt/src/connectors.rs` |

---

## 7. Hosted Codex Apps MCP

| Purpose | Route | Transport | Auth / headers | Source |
|---|---|---|---|---|
| Hosted Apps MCP | `B/ps/mcp` | MCP Streamable HTTP | ChatGPT auth; `X-OpenAI-Product-Sku: codex`; optional `originator` | `codex-rs/codex-mcp/src/mcp/mod.rs` |

This is an MCP endpoint, not a set of normal REST calls. Traffic includes protocol messages such as `initialize`, `tools/list`, `tools/call`, `resources/list`, `resources/read`, and event/elicitation messages.

---

## 8. Remote plugin marketplace APIs

| Purpose | Route | Method / params | Auth | Source |
|---|---|---|---|---|
| Suggested plugins | `B/ps/plugins/suggested/codex?scope=GLOBAL` | GET | ChatGPT auth + product SKU | `codex-rs/core-plugins/src/remote.rs` |
| Plugin search | `B/ps/plugins/search?q=...&scope=...&limit=...&pageToken=...` | GET | same | `remote/search.rs` |
| Plugin list | `B/ps/plugins/list?scope=...&limit=200&collection=...&pageToken=...` | GET | same | `remote.rs` |
| Shared workspace plugins | `B/ps/plugins/workspace/shared?limit=200&pageToken=...` | GET | same | `remote.rs` |
| Installed plugins | `B/ps/plugins/installed?...` | GET | same | `remote.rs` |
| Plugin detail | `B/ps/plugins/{plugin_id}` | GET; optional `includeDownloadUrls=true` | same | `remote.rs` |
| Plugin skill | `B/ps/plugins/{plugin_id}/skills/{skill_name}` | GET | same | `remote.rs` |
| Install | `B/ps/plugins/{plugin_id}/install?includeAppsNeedingAuth=true` | POST; optional `install_attempt_id` | same | `remote.rs` |
| Uninstall | `B/ps/plugins/{plugin_id}/uninstall` | POST | same | `remote.rs` |
| Workspace-created shares | `B/ps/plugins/workspace/created` | GET | same | `remote/share.rs` |
| Update share targets | `B/ps/plugins/{plugin_id}/shares` | PUT JSON | same | `remote/share.rs` |
| Get upload URL | `B/public/plugins/workspace/upload-url` | POST JSON filename/type/size | same | `remote/share.rs` |
| Create/update workspace plugin | `B/public/plugins/workspace[/<id>]` | POST JSON | same | `remote/share.rs` |
| Delete workspace plugin | `B/public/plugins/workspace/{id}` | DELETE | same | `remote/share.rs` |

Plugin bundle upload uses a backend-provided presigned storage URL, typically a direct `PUT` with storage-specific headers; do not attach OpenAI account credentials to arbitrary presigned destinations.

---

## 9. OpenAI/ChatGPT file upload for Apps tools

Flow:

1. Create file record.
2. Upload bytes to a presigned blob URL.
3. Finalize the upload.

| Step | Route | Method / body | Auth |
|---|---|---|---|
| Create | `POST B/files` | `{file_name, file_size, use_case:"codex", ...optional hosted context}` | ChatGPT auth + account ID |
| Blob upload | `<upload_url returned by server>` | `PUT` raw bytes; `x-ms-blob-type: BlockBlob`, `Content-Length`, request ID | **No OpenAI credential to arbitrary storage host** |
| Finalize | `POST B/files/{file_id}/uploaded` | `{}` or C2PA metadata | ChatGPT auth |

Source map: `codex-rs/codex-api/src/files.rs`, `codex-rs/core/src/mcp_openai_file.rs`.

---

## 10. Authentication APIs

Default issuer: `AUTH = https://auth.openai.com`.

| Purpose | URL | Method / main parameters | Cookies? | Source |
|---|---|---|---|---|
| Browser authorize | `GET AUTH/oauth/authorize` | `response_type=code`, `client_id`, `redirect_uri`, PKCE challenge, `state`, scopes, Codex-specific extra params | Browser may use normal web auth cookies; CLI itself does not replay browser session as API auth | `codex-rs/login/src/server.rs`; `oauth/authorization.rs` |
| Authorization-code exchange | `POST AUTH/oauth/token` | form: `grant_type=authorization_code`, `client_id`, `code`, `redirect_uri`, `code_verifier` | no special CLI auth cookie required | `login/src/oauth/client.rs`; `server.rs` |
| Refresh token | `POST AUTH/oauth/token` | JSON: `grant_type=refresh_token`, `client_id`, `refresh_token` | no | `login/src/auth/manager.rs`; `oauth/client.rs` |
| ID-token → API-key-style token | `POST AUTH/oauth/token` | token-exchange grant, `requested_token=openai-api-key`, `subject_token=<id_token>` | no | `login/src/server.rs` |
| Revoke | `POST AUTH/oauth/revoke` | JSON `token`, `token_type_hint`, optional `client_id` | no | `login/src/auth/revoke.rs` |
| Device user code | `POST AUTH/api/accounts/deviceauth/usercode` | JSON `{client_id}` | no | `login/src/device_code_auth.rs` |
| Device polling | `POST AUTH/api/accounts/deviceauth/token` | `{device_auth_id,user_code}` | no | same |
| Device verification page | `GET AUTH/codex/device` | browser UI | browser auth | same |
| PAT identity | `GET AUTH/api/accounts/v1/user-auth-credential/whoami` | Bearer PAT | no | `login/src/auth/personal_access_token.rs` |
| Agent registration | `POST AUTH/api/accounts/v1/agent/register` | public key, capabilities, ABOM, etc. | bearer ChatGPT credential | `codex-rs/agent-identity/src/lib.rs` |
| Agent task registration | `POST AUTH/api/accounts/v1/agent/{runtime_id}/task/register` | timestamp + signature | signed agent key material | same |
| Agent JWKS | ChatGPT backend `.../wham/agent-identities/jwks` (or configured equivalent) | GET | normal routing | same |

OAuth browser scope currently includes `openid profile email offline_access api.connectors.read api.connectors.invoke`.

---

## 11. ChatGPT cookies

Codex's built-in ChatGPT cookie store is intentionally infrastructure-only. It may retain/replay allowlisted routing/Cloudflare cookies such as:

- `__cf_bm`
- `__cflb`
- `__cfruid`
- `__cfseq`
- `__cfwaitingroom`
- `_cfuvid`
- `cf_clearance`
- `cf_ob_info`
- `cf_use_ob`
- `cf_chl_*`
- `__oailb`

It explicitly avoids storing ChatGPT account/session/auth cookies in the shared jar.

Source: `codex-rs/http-client/src/chatgpt_cloudflare_cookies.rs`.

---

## 12. App-server remote-control APIs (optional)

| Purpose | Production route | Method / auth | Main body | Source |
|---|---|---|---|---|
| Server enroll | `POST B/wham/remote/control/server/enroll` | normal ChatGPT auth + `x-codex-installation-id` | `{name,os,arch,app_server_version,installation_id}` | `app-server-transport/.../remote_control/server_api.rs` |
| Token refresh | `POST B/wham/remote/control/server/refresh` | normal ChatGPT auth + installation ID | `{server_id,installation_id}` | same |
| Remote-control channel | `WSS B/wham/remote/control/server` | remote-control enrollment auth | JSON-RPC envelope stream | `remote_control/websocket.rs`; `protocol.rs` |
| Pair | `POST B/wham/remote/control/server/pair` | `Bearer <remote_control_token>` | `{manual_code:bool}` | `remote_control/enroll.rs` |
| Pair status | `POST B/wham/remote/control/server/pair/status` | `Bearer <remote_control_token>` | pairing code or manual pairing code | same |

---

## 13. Feedback / telemetry

- Interactive feedback is sent through a Sentry envelope transport rather than the Responses/ChatGPT model API.
- OTLP telemetry can target a user/configured collector and therefore has no single fixed OpenAI URL.

Source: `codex-rs/feedback/src/lib.rs`, `feedback/src/upload.rs`, `codex-rs/otel/*`.

---

## 14. Open-ended network surfaces

These cannot be covered by a fixed OpenAI route table:

- custom model-provider `base_url`
- custom provider OAuth endpoints
- arbitrary HTTP MCP servers
- arbitrary MCP OAuth authorization/token endpoints
- Amazon Bedrock/AWS endpoints
- Ollama / LM Studio local endpoints
- presigned file/plugin storage URLs
- backend-returned download URLs
- OTLP collector URLs
- Git remotes and plugin/package sources

A gateway must therefore distinguish trusted OpenAI/ChatGPT routes from arbitrary third-party destinations before injecting credentials.

---

## 15. Proxy scope recommendation

### Model-only gateway

For the architecture discussed in this project:

```text
Codex
  ├── C/model traffic ──> YOUR GATEWAY ──> upstream OpenAI/ChatGPT
  └── B/product traffic ─────────────────> ChatGPT unchanged
```

Recommended first implementation:

1. `POST /responses` HTTP/SSE
2. `WSS /responses`
3. `GET /models`
4. `POST /alpha/search`
5. user authentication at the gateway
6. server-side upstream credential injection
7. passive SSE/WS accounting parser
8. preserve unknown headers/body/event fields

Optional second phase:

- memories
- images
- realtime

Do **not** proxy or reimplement B unless you intend to reproduce ChatGPT product/account features such as usage, workspace routing, Apps/MCP, plugins, cloud tasks, or remote control.

### Key configuration distinction

```text
model_provider.base_url / openai_base_url
    -> replaces C/model endpoint

chatgpt_base_url
    -> B/product/account endpoint
    -> unchanged by model-provider override unless explicitly changed
```
