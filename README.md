# unicodex

A Codex-only API relay designed to work with a [custom Codex build](https://github.com/dwuggh/codex/tree/custom-chatgpt-backend).

## Status

Codex CLI has been verified to work with the custom build linked above. Full desktop compatibility has not been verified; see [Limitations](#limitations) for the client changes required.

## Usage

### Server setup

1. Install the Rust toolchain specified in `rust-toolchain.toml` and build the server:

   ```sh
   cargo build --release
   cp config.example.yaml config.yaml
   ```

2. Edit `config.yaml`, using [config.example.yaml](config.example.yaml) as a reference:
   - Give each inbound a unique gateway key and user name.
   - Set `weekly_credits` to a quoted decimal allowance or `"unlimited"`.
   - Configure an outbound with a server-owned copy of the upstream account's `auth.json`, or a token supplied through `token_env`.
   - Map the inbound IDs to the chosen outbound in `routing.rules`.

   Unicodex may refresh tokens and update the server's auth file. Use a separate copy from the client's login file. Relative database and auth-file paths resolve against the directory containing `config.yaml`.

3. Start the server:

   ```sh
   RUST_LOG=info,unicodex=debug ./target/release/unicodex --config config.yaml
   ```

   The credit database is created automatically. Credits are calculated in Rust and stored as numeric charges in a local Turso database through Toasty.

4. For remote access, expose the listener through an HTTPS reverse proxy. Forward `/backend-api/` to Unicodex without stripping the path, and enable WebSocket upgrades for inference requests. The example listens on `127.0.0.1:8787`.

### Client setup

Install the [custom Codex build](https://github.com/dwuggh/codex/tree/custom-chatgpt-backend) and retain a valid ChatGPT login in the client’s Codex home directory. This setup uses ChatGPT authentication mode; the gateway key authenticates requests to Unicodex, while the server supplies its own credentials upstream.

Add the following to Codex's `config.toml`, normally under `~/.codex/` or the directory selected by `CODEX_HOME`:

```toml
cli_auth_credentials_store = "file" # unicodex will not modify your `auth.json`

# to disable unicodex, comment out following 2 lines
chatgpt_base_url = "https://gateway.example.com/backend-api"
model_provider = "unicodex"

[model_providers.unicodex]
name = "unicodex"
base_url = "https://gateway.example.com/backend-api/codex"
wire_api = "responses"
requires_openai_auth = true
supports_websockets = true
http_headers = { Authorization = "Bearer YOUR_GATEWAY_KEY" }
```

Replace `gateway.example.com` with your gateway's address and `YOUR_GATEWAY_KEY` with the matching inbound key. Keep the two URL paths as shown: `chatgpt_base_url` handles account and product requests, while the provider's `base_url` handles Codex model requests.

Start `codex` in a project directory and send a message. Use `/status` to inspect usage. For a limited user, Unicodex reports local credit consumption against that user's allowance within the upstream account's weekly reset window. Unlimited users retain the upstream usage display.

If requests fail, check the server logs. A `no_matching_inbound` rejection indicates that the gateway key did not match an inbound; `unsupported_route` indicates that strict routing rejected the path. Insufficient local credits return HTTP 429. Database failures or an unavailable weekly reset window can prevent limited users from starting inference.

## Limitations

### Why Codex needs a patch

Setting a custom model provider alone does not redirect and authenticate every ChatGPT backend request. In the upstream revision this fork is based on, the [cloud-task URL validator](https://github.com/openai/codex/blob/afb436df8b70bb5bc57b86d9a3e829968988cd21/codex-rs/cloud-tasks/src/util.rs#L44-L77) restricts destinations to official ChatGPT hosts. The [MCP trust check](https://github.com/openai/codex/blob/afb436df8b70bb5bc57b86d9a3e829968988cd21/codex-rs/codex-mcp/src/mcp/mod.rs#L359-L389) also limits which origins can use ChatGPT session authentication; untrusted MCP destinations [fall back to OAuth](https://github.com/openai/codex/blob/afb436df8b70bb5bc57b86d9a3e829968988cd21/codex-rs/codex-mcp/src/mcp/mod.rs#L415-L420). These checks protect saved credentials, but they conflict with using Unicodex as a custom ChatGPT backend.

The custom build changes that behavior in two ways:

- It permits a custom HTTPS backend in the relevant cloud-task and MCP checks, including trusting an MCP origin that matches the configured ChatGPT backend.
- It applies the selected provider's configured headers to HTTP and WebSocket requests on the configured backend origin. Those headers override generated request headers, so the gateway receives `Authorization: Bearer YOUR_GATEWAY_KEY` instead of the client's ChatGPT token.

Without those changes, some requests may be rejected by the client before reaching Unicodex, while others reach the gateway without the required key. Changing nginx routes cannot fix a rejection that happens inside the client. Use the custom build only with a backend you trust.

### Desktop and upstream compatibility


Unicodex relies on the upstream Codex and ChatGPT protocols. Upstream changes can require updates to routing, authentication, or usage parsing. Local credit accounting does not create separate upstream accounts or entitlements; other account features still use the selected outbound account.
