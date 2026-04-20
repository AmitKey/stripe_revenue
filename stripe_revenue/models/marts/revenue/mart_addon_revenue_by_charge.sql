-- Charge-level P&L. One row per Stripe charge with every revenue and cost component as its own column.
-- gross_charge_amount comes from the charges table (authoritative), not from summing activity events.
-- net_revenue = gross_charge minus all outflows. net_take_rate = what fraction DoorLoop keeps.
-- is_settled turns true only once a connect_transfer event has landed (ch_EEE is still unsettled).
-- Orphan activity is excluded — those rows have no charge context to aggregate into.
with events as (
    select * from {{ ref('mart_addon_revenue_events') }}
    where not is_orphan_activity  -- orphan rows have no charge context to aggregate into
),

by_charge as (
    select
        charge_id,
        charged_at,
        date_trunc('month', charged_at)                                             as charge_month,

        -- Tenant and product context
        max(dbtenant_ids)                                                           as dbtenant_ids,
        max(addon_types)                                                            as addon_types,
        max(charge_status)                                                          as charge_status,

        -- Gross income: authoritative from raw_stripe_charges, not the activity event
        max(charge_amount)                                                          as gross_charge_amount,

        -- Outflows: all from activity events (transfers, fees, refunds)
        sum(case when event_subtype = 'connect_transfer' then amount else 0 end)   as connect_transfer_amount,
        sum(case when event_subtype = 'processing_fee'   then amount else 0 end)   as processing_fee_amount,
        sum(case when event_subtype = 'interchange'      then amount else 0 end)   as interchange_fee_amount,
        sum(case when event_subtype = 'dispute_fee'      then amount else 0 end)   as dispute_fee_amount,
        sum(case when event_subtype = 'refund'           then amount else 0 end)   as refund_amount,
        sum(case when event_subtype = 'reversal'         then amount else 0 end)   as reversal_amount,

        -- Net revenue = gross income (charges table) minus all activity outflows
        max(charge_amount)
            - sum(case when event_subtype = 'connect_transfer' then amount else 0 end)
            - sum(case when event_subtype = 'processing_fee'   then amount else 0 end)
            - sum(case when event_subtype = 'interchange'      then amount else 0 end)
            - sum(case when event_subtype = 'dispute_fee'      then amount else 0 end)
            - sum(case when event_subtype = 'refund'           then amount else 0 end)
            - sum(case when event_subtype = 'reversal'         then amount else 0 end) as net_revenue,

        case
            when max(charge_amount) > 0
            then (
                max(charge_amount)
                - sum(case when event_subtype = 'connect_transfer' then amount else 0 end)
                - sum(case when event_subtype = 'processing_fee'   then amount else 0 end)
                - sum(case when event_subtype = 'interchange'      then amount else 0 end)
                - sum(case when event_subtype = 'dispute_fee'      then amount else 0 end)
                - sum(case when event_subtype = 'refund'           then amount else 0 end)
                - sum(case when event_subtype = 'reversal'         then amount else 0 end)
            ) / max(charge_amount)
            else null
        end                                                                         as net_take_rate,

        -- Settlement: true once a connect_transfer event has landed
        count(case when event_subtype = 'connect_transfer' then 1 end) > 0         as is_settled,

        -- Days from original charge to first transfer (null if not yet settled)
        min(case when event_subtype = 'connect_transfer'
                 then days_since_charge end)                                        as days_to_first_transfer,

        count(balance_transaction_id)                                               as event_count,

        -- Data quality
        bool_or(is_currency_conversion_failed)                                      as is_original_currency_amount_missing,
        bool_or(has_multiple_addon_types)                                           as has_multiple_addon_types

    from events
    group by charge_id, charged_at
)

select * from by_charge
