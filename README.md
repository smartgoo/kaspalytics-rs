Prototyping Kaspalytics analytics services in Rust.

# Notes
Thinking of 4 core components:
- `cli` app for misc/ad-hoc analysis and utility functions
- `indexer` that indexes select block & transaction data to an in process cache, with some data saved to DB
- `webapi` that provides SSE, pushing from cache to front end running in user's browser
- `daemon` that runs `indexer` and `webapi`

Might need some communication between `indexer` and `webapi` (pub/sub, broadcast, etc). Or a way for cache to emit cache update event notifications.

`sqlx-cli` for db migrations ([sqlx-cli README](https://github.com/launchbadge/sqlx/blob/main/sqlx-cli/README.md))

Lints:
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --tests --benches --examples -- -D warnings`