#!/usr/bin/env bash
# dbt-visigrid-demo: end-to-end verification with signed proofs
#
# Prerequisites: vgrid, dbt-duckdb, python3
# Output: warehouse_out/ with pass + fail artifacts
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# Helper: export warehouse_daily_totals from DuckDB to CSV
export_warehouse() {
    local outfile="$1"
    python3 -c "
import duckdb, csv, sys
con = duckdb.connect('target/demo.duckdb', read_only=True)
rows = con.execute('''
    select
        strftime(date, '%Y-%m-%d') as date,
        currency,
        source_account,
        printf('%.6f', total_gross::double) as total_gross,
        printf('%.6f', total_fee::double) as total_fee,
        printf('%.6f', total_net::double) as total_net,
        transaction_count::int as transaction_count
    from warehouse_daily_totals
    order by date, currency, source_account
''').fetchall()
cols = ['date','currency','source_account','total_gross','total_fee','total_net','transaction_count']
w = csv.writer(open('$outfile', 'w', newline=''))
w.writerow(cols)
for r in rows:
    w.writerow(r)
con.close()
"
}

echo "=== Step 1: Export truth seeds ==="
rm -rf seeds/ warehouse_out/ target/
mkdir -p seeds warehouse_out

vgrid export truth --transactions fixtures/truth_transactions.csv --out seeds/
echo ""

echo "=== Step 2: Seed DuckDB ==="
dbt seed --profiles-dir .
echo ""

echo "=== Step 3: PASS run (no mismatch) ==="
dbt run --profiles-dir . --vars '{mismatch: false}'

export_warehouse warehouse_out/warehouse_daily_totals.pass.csv

vgrid verify totals \
    seeds/truth_daily_totals.csv \
    warehouse_out/warehouse_daily_totals.pass.csv \
    --tolerance 0.01 \
    --output warehouse_out/verify.pass.json \
    --diff warehouse_out/diffs.pass.csv \
    --sign --proof warehouse_out/proof.pass.json

echo ""
echo "--- PASS run complete ---"
echo ""

echo "=== Step 4: FAIL run (deliberate mismatch) ==="
dbt run --profiles-dir . --vars '{mismatch: true}'

export_warehouse warehouse_out/warehouse_daily_totals.fail.csv

# This is expected to fail (exit 1) — don't stop the script
set +e
vgrid verify totals \
    seeds/truth_daily_totals.csv \
    warehouse_out/warehouse_daily_totals.fail.csv \
    --tolerance 0.01 \
    --output warehouse_out/verify.fail.json \
    --diff warehouse_out/diffs.fail.csv \
    --sign --proof warehouse_out/proof.fail.json
FAIL_EXIT=$?
set -e

echo ""
echo "--- FAIL run complete (exit code: $FAIL_EXIT) ---"
echo ""

echo "=== Artifacts ==="
ls -la warehouse_out/
echo ""
echo "=== Summary ==="
echo "PASS verify.json:"
python3 -c "import json; d=json.load(open('warehouse_out/verify.pass.json')); print(f\"  status={d['status']}  matched={d['summary']['matched']}\")"
echo "FAIL verify.json:"
python3 -c "import json; d=json.load(open('warehouse_out/verify.fail.json')); print(f\"  status={d['status']}  mismatched={d['summary']['mismatched']}\")"
echo ""
echo "Done. All artifacts in warehouse_out/"
