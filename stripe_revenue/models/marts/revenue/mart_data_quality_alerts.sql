-- One row per flagged data issue. Five alert types, each with a severity (warning or info).
-- Built so Finance can query known problems directly without digging into raw data.
-- Alert types: orphan_activity, currency_conversion_failed, multi_addon_charge,
--              missing_journal_entry, pending_settlement.
with events as (
    select * from {{ ref('mart_addon_revenue_events') }}
),

charges as (
    select * from {{ ref('mart_addon_revenue_by_charge') }}
),

enriched_charges as (
    select * from {{ ref('int_charges_enriched') }}
),

-- Alert 1: Orphan activity — financial event has no matching charge record
--   Includes ch_FFF (dispute_fee) and ch_GGG (refund) from raw_stripe_activity.
--   These amounts flow through DoorLoop's Stripe balance but cannot be attributed
--   to a tenant or addon type.
orphan_activity as (
    select
        'orphan_activity'                                               as alert_type,
        'warning'                                                       as severity,
        balance_transaction_id,
        charge_id,
        event_at,
        event_subtype,
        amount,
        null::varchar                                                   as addon_types,
        null::varchar                                                   as dbtenant_ids,
        'Activity event has no matching charge — tenant context unknown; '
        || 'may indicate a deleted charge or out-of-band Stripe event'  as description
    from events
    where is_orphan_activity
),

-- Alert 2: Currency conversion failure — original amount is null despite non-USD currency
--   Affects txn_008 (ch_CCC, CAD charge). USD amount was populated by Stripe but
--   the source of truth in CAD is missing. Flag before including in revenue totals.
currency_failures as (
    select
        'currency_conversion_failed'                                    as alert_type,
        'warning'                                                       as severity,
        balance_transaction_id,
        charge_id,
        event_at,
        event_subtype,
        amount,
        addon_types,
        dbtenant_ids,
        'Non-USD charge with no original currency amount logged; '
        || 'USD conversion value may be inaccurate'                     as description
    from events
    where is_currency_conversion_failed
),

-- Alert 3: Multi-addon charge — one Stripe charge spans payments and screening
--   Affects ch_DDD ($200). Revenue cannot be split per addon without a source-level
--   amount allocation. Excluded from mart_addon_revenue_monthly per-addon totals.
multi_addon_charges as (
    select
        'multi_addon_charge'                                            as alert_type,
        'info'                                                          as severity,
        null::varchar                                                   as balance_transaction_id,
        charge_id,
        charged_at                                                      as event_at,
        null::varchar                                                   as event_subtype,
        gross_charge_amount                                             as amount,
        addon_types,
        dbtenant_ids,
        'Charge spans multiple addon types (' || addon_types || '); '
        || 'per-addon revenue attribution is ambiguous'                 as description
    from charges
    where has_multiple_addon_types
),

-- Alert 4: Missing journal entry — charge has no internal record mapping it to a tenant
--   These charges cannot be attributed to a dbtenant_id or addon_type.
missing_journal_entry as (
    select
        'missing_journal_entry'                                         as alert_type,
        'warning'                                                       as severity,
        null::varchar                                                   as balance_transaction_id,
        charge_id,
        charged_at                                                      as event_at,
        null::varchar                                                   as event_subtype,
        charge_amount                                                   as amount,
        null::varchar                                                   as addon_types,
        null::varchar                                                   as dbtenant_ids,
        'Charge has no matching journal entry — '
        || 'addon type and tenant are unknown'                          as description
    from enriched_charges
    where is_unmatched_charge
),

-- Alert 5: Pending settlement — charge has no connect_transfer event yet.
--   The property manager transfer hasn't landed; net_revenue on these charges equals gross and is overstated.

pending_settlement as (
    select
        'pending_settlement'                                            as alert_type,
        'info'                                                          as severity,
        null::varchar                                                   as balance_transaction_id,
        charge_id,
        charged_at                                                      as event_at,
        null::varchar                                                   as event_subtype,
        gross_charge_amount                                             as amount,
        addon_types,
        dbtenant_ids,
        'Charge has not settled yet; '
        || 'use net_revenue_settled and net_revenue_in_flight rather than net_revenue' as description
    from charges
    where not is_settled
),

all_alerts as (
    select * from orphan_activity
    union all
    select * from currency_failures
    union all
    select * from multi_addon_charges
    union all
    select * from missing_journal_entry
    union all
    select * from pending_settlement
)

select
    alert_type,
    severity,
    balance_transaction_id,
    charge_id,
    event_at,
    event_subtype,
    amount,
    addon_types,
    dbtenant_ids,
    description
from all_alerts
order by
    case severity when 'warning' then 1 when 'info' then 2 else 3 end,
    alert_type,
    event_at
