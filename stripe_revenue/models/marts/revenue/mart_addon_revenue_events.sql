-- Enriched event-level table. One row per Stripe balance transaction.
-- Includes all activity, orphan or not. The is_orphan_activity flag is surfaced as a column
-- so consumers can filter themselves if needed.
with base as (
    select * from {{ ref('int_activity_classified') }}
),

enriched as (
    select
        -- Keys
        balance_transaction_id,
        charge_id,

        -- Timing
        event_at,
        charged_at,
        date_trunc('month', event_at)                       as event_month,
        date_trunc('month', charged_at)                     as charge_month,
        date_diff('day', charged_at, event_at)              as days_since_charge,

        -- Event classification
        event_type,
        event_subtype,
        flow_direction,
        case event_subtype
            when 'charge'           then 'gross_charge'
            when 'processing_fee'   then 'processing_fee'
            when 'interchange'      then 'interchange_fee'
            when 'connect_transfer' then 'connect_transfer'
            when 'dispute_fee'      then 'dispute_fee'
            when 'refund'           then 'refund'
            when 'reversal'         then 'reversal'
            else                         'other'
        end                                                 as revenue_category,

        -- Amounts (all USD)
        amount,
        net_amount,
        amount_original_currency,
        amount_original_currency is not null                as has_original_currency_amount,

        -- Tenant and product context
        dbtenant_ids,
        addon_types,
        charge_amount,
        expected_transfer_amount,
        charge_status,

        -- Data quality flags
        is_currency_conversion_failed,
        has_multiple_addon_types,
        is_orphan_activity
    from base
)

select * from enriched
