-- Every Stripe event enriched with charge context and a signed net amount. One row per balance transaction.
-- net_amount is positive for inflows and negative for outflows — summing it gives net revenue directly.
-- Events with no matching charge (ch_FFF dispute, ch_GGG refund) are kept and flagged as is_orphan_activity.
with activity as (
    select * from {{ ref('stg_stripe__activity') }}
),

charges as (
    select * from {{ ref('int_charges_enriched') }}
),

classified as (
    select
        -- Keys
        a.balance_transaction_id,
        a.charge_id,

        -- Timing
        a.event_at,
        c.charged_at,

        -- Event classification
        a.event_type,
        a.event_subtype,
        a.flow_direction,

        -- Amounts
        a.amount,
        -- Positive for inflows, negative for outflows
        case
            when a.flow_direction = 'inflow'  then a.amount
            when a.flow_direction = 'outflow' then -a.amount
            else 0
        end                          as net_amount,
        a.currency,
        a.amount_original_currency,

        -- Charge and tenant context (null for orphan activity)
        c.charge_amount,
        c.charge_status,
        c.dbtenant_ids,
        c.addon_types,
        c.has_multiple_addon_types,

        -- Data quality flags
        a.is_currency_conversion_failed,
        -- True for activity rows with no matching charge (e.g. ch_FFF dispute, ch_GGG refund)
        c.charge_id is null          as is_orphan_activity
    from activity a
    left join charges c on a.charge_id = c.charge_id
)

select * from classified
