


## structure
- common
  - ledger
- inbounds
  - codex
- outbounds
  - codex
- config: parse config file
- cli: take care of cli arg parsing, with clap


## ledger
## config example

```yaml
server:
  listen: "127.0.0.1:8787"

inbounds:
  - id: user-a
    name: Alice
    type: codex
    key: "choose-alice-secret"

  - id: user-b
    name: Bob
    type: codex
    key: "choose-bob-secret"

  - id: user-c
    name: Carol
    type: codex
    key: "choose-carol-secret"

outbounds:
  - id: account-1
    type: codex
    auth_file: /path/to/account-1/auth.json

  - id: account-2
    type: codex
    auth_file: /path/to/account-2/auth.json

routing:
  rules:
    - inbound_ids: [user-a, user-b]
      outbound_id: account-1

    - inbound_ids: [user-c]
      outbound_id: account-2
```
