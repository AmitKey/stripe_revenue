-- Cleans raw Stripe activity events. One row per balance transaction (charge, fee, transfer, refund, dispute).
-- Flow direction is re-derived from business logic — only charge events are inflows, everything else
-- is an outflow. The source field was unreliable (txn_009 was wrongly tagged as inflow).
-- Rows where currency is non-USD but original amount is null are flagged as currency conversion failures.
with source as (
    select * from {{ source('stripe_raw', 'raw_stripe_activity') }}
),

corrected as (
    select
        balance_transaction_id,
        charge_id,
        event_at,
        event_type,
        event_subtype,
        amount,
        -- Derive flow direction from business logic: only charges are income.
        -- The source flow_direction field is unreliable (e.g. txn_009 is tagged inflow
        -- despite being a connect_transfer). Ignore it entirely.
        case
            when event_subtype = 'charge' then 'inflow'
            else                               'outflow'
        end                                                     as flow_direction,
        currency,
        amount_original_currency,
        -- Flag rows where currency conversion failed (non-USD with null original amount)
        (currency != 'USD' and amount_original_currency is null) as is_currency_conversion_failed
    from source
)

select * from corrected
