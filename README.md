# dbt-visigrid-demo

End-to-end financial verification demo: dbt + DuckDB + VisiGrid proof signing.

## Prerequisites

```
pip install dbt-duckdb duckdb
cargo install --path ../visigrid/app/crates/cli  # or download vgrid binary
```

## Run

```
./demo.sh
```

Produces a PASS run (clean match) and a FAIL run (deliberate $0.02 mismatch), with signed Ed25519 proofs.

## Output

All artifacts land in `warehouse_out/`:

| File | Description |
|------|-------------|
| `verify.pass.json` | Verification result (status: pass) |
| `verify.fail.json` | Verification result (status: fail) |
| `diffs.pass.csv` | Empty — no mismatches |
| `diffs.fail.csv` | Rows that differ |
| `proof.pass.json` | Signed proof envelope (pass) |
| `proof.pass.sig` | Raw Ed25519 signature |
| `proof.fail.json` | Signed proof envelope (fail) |
| `proof.fail.sig` | Raw Ed25519 signature |

## How it works

1. `vgrid export truth` converts fixture transactions → dbt seeds (deterministic CSVs)
2. `dbt seed` loads seeds into DuckDB
3. `dbt run` aggregates transactions → `warehouse_daily_totals` (with optional mismatch toggle)
4. `vgrid verify totals` compares truth vs warehouse, signs proof with Ed25519
