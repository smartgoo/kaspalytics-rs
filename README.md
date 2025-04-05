Work In Progress - Prototyping Kaspalytics analytics services in Rust.

Short term, thinking of these components:
- `cli` app for misc/ad-hoc analysis and utility functions
- `daemon` that runs `ingest` (queries DAG, writes to DB) and `analyzer`

Longer term:
- `ingest` -> `bus` -> `plugins` 
- `webapi` that provides real time data to front end via websockets/SSE

`sqlx-cli` for db migrations ([sqlx-cli README](https://github.com/launchbadge/sqlx/blob/main/sqlx-cli/README.md))

Lints:
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --tests --benches --examples -- -D warnings`

Roadmap:
1. Fully migrate from Kaspalytics Python before Crescendo HF
2. Prep for 10 BPS
3. Overhaul DB model
4. Refactor analyzer services to event driven?
5. Add websocket and/or SSE and expose real time data to FE (DAG visualizer, dashboards, etc.)