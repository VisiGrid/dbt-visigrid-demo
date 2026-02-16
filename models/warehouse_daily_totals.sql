-- Aggregate truth_transactions into daily totals.
-- Matches the truth_daily_totals schema exactly:
--   date, currency, source_account, total_gross, total_fee, total_net, transaction_count
--
-- When var('mismatch') is true, adds $0.02 to total_net on the earliest date
-- to simulate a warehouse discrepancy.

with txn as (
    select
        occurred_at as date,
        currency,
        source_account,
        direction,
        amount_gross,
        fee_amount,
        amount_net
    from {{ ref('truth_transactions') }}
),

aggregated as (
    select
        date,
        currency,
        source_account,
        -- Gross: credits positive, debits negative
        sum(case when direction = 'credit' then amount_gross
                 when direction = 'debit'  then -amount_gross
                 else 0 end) as total_gross,
        -- Fee: always non-negative, sum directly
        sum(fee_amount) as total_fee,
        -- Net: credits positive, debits negative
        sum(case when direction = 'credit' then amount_net
                 when direction = 'debit'  then -amount_net
                 else 0 end) as total_net,
        count(*) as transaction_count
    from txn
    group by date, currency, source_account
),

with_mismatch as (
    select
        date,
        currency,
        source_account,
        total_gross,
        total_fee,
        case
            when {{ var('mismatch') }}
                 and date = (select min(date) from aggregated)
            then total_net + 0.020000
            else total_net
        end as total_net,
        transaction_count
    from aggregated
)

select * from with_mismatch
order by date, currency, source_account
