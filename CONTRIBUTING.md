# Contributing

## Workflow

1. Create a feature branch from `main`:
   - `feat/<topic>`
   - `fix/<topic>`
2. Keep PRs focused and small enough to review.
3. Open a PR and request review from your teammate.
4. Merge only after review and passing checks.

## Commit Convention

- Use concise prefixes:
  - `feat:`
  - `fix:`
  - `docs:`
  - `chore:`
- Example: `feat: add fixed-bucket parquet builder command`

## Local Validation Before PR

Run at least:

```bash
python -m polymarket_collector --help
python -m polymarket_collector run-primary --help
python -m polymarket_collector build-parquet --help
```

If your change touches runtime logic, also run one short dry run with a test `--data-root`.

## Data and Secrets

- Never commit runtime data under `data/`, `data_demo/`, `data_test/`.
- Never commit `.env` or private keys.
- Keep config examples in `.env.example`.

## Ownership

- Collector runtime / failover: both maintainers.
- Data schema and parquet build: both maintainers.
- Docs must be updated in the same PR when behavior changes.
