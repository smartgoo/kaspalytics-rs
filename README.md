Work In Progress - Prototyping Kaspalytics analytics services in Rust.

Short term, thinking of these components:
- `cli` app for misc/ad-hoc analysis and utility functions
- `daemon` that runs a `listener` (listens to DAG, indexes select block & transaction data to an in process cache) and `analyzer` (runs over in process cache, analyzes, saves to DB)

Longer term:
- `webapi` that provides real time data to front end via websockets/SSE
- Might need some communication between `indexer` and `webapi` (pub/sub, broadcast, etc)

`sqlx-cli` for db migrations ([sqlx-cli README](https://github.com/launchbadge/sqlx/blob/main/sqlx-cli/README.md))

Lints:
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --tests --benches --examples -- -D warnings`

Roadmap:
1. Fully migrate from existing Kaspalytics Python analytics before Crescendo HF
2. Overhaul DB model
3. Refine listener/analyzer services to better support real time data
4. Add websocket or SSE and expose real time data on FE
5. Add DAG visualizer to FE