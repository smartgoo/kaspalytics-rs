www.kaspalytics.com indexer & analytics services.

Two binary crates & one library crate:
- `cli`
- `daemon`
- `utils`

`sqlx-cli` for db migrations ([sqlx-cli README](https://github.com/launchbadge/sqlx/blob/main/sqlx-cli/README.md))

Lints:
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --tests --benches --examples -- -D warnings`
