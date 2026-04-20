-- Monthly revenue roll-up by addon type. One row per (charge month, addon type).
-- Multi-addon charges (ch_DDD) are included with addon_type = 'payments,screening'.
-- Revenue is real regardless of attribution ambiguity — excluding it would understate income.
-- Groups by charge_month (when income was recognized) not event_month (when fees landed).
-- All orphan activity (dispute fees and refunds) rolls up under addon_type = 'unattributed'
-- so net revenue is never overstated.
with charges as (
    select * from {{ ref('mart_addon_revenue_by_charge') }}
),

attributed_monthly as (
    select
        charge_month                            as recognized_month,
        addon_types                             as addon_type,

        count(distinct charge_id)               as charge_count,
        count(case when is_settled
              then charge_id end)               as settled_charge_count,
        sum(gross_charge_amount)                as gross_charges,
        sum(connect_transfer_amount)            as connect_transfers_out,
        sum(processing_fee_amount)              as processing_fees,
        sum(interchange_fee_amount)             as interchange_fees,
        sum(dispute_fee_amount)                 as dispute_fees,
        sum(refund_amount)                      as refunds,
        sum(reversal_amount)                    as reversals,
        sum(net_revenue)                        as net_revenue,
        case
            when sum(gross_charge_amount) > 0
            then sum(net_revenue) / sum(gross_charge_amount)
            else null
        end                                     as net_take_rate

    from charges
    group by charge_month, addon_types
),

-- Orphan activity: real costs (dispute fees, refunds) with no matching charge.
-- Cannot attribute to a tenant or addon — included here so total net revenue
-- is not overstated. take_rate is null because there is no gross income to divide by.
orphan_events as (
    select * from {{ ref('mart_addon_revenue_events') }}
    where is_orphan_activity
),

orphan_monthly as (
    select
        date_trunc('month', event_at)           as recognized_month,
        'unattributed'                          as addon_type,

        0                                       as charge_count,
        0                                       as settled_charge_count,
        0                                       as gross_charges,
        0                                       as connect_transfers_out,
        0                                       as processing_fees,
        0                                       as interchange_fees,
        sum(case when event_subtype = 'dispute_fee'
                 then amount else 0 end)        as dispute_fees,
        sum(case when event_subtype = 'refund'
                 then amount else 0 end)        as refunds,
        0                                       as reversals,
        -sum(amount)                            as net_revenue,
        null                                    as net_take_rate

    from orphan_events
    group by date_trunc('month', event_at)
),

combined as (
    select * from attributed_monthly
    union all
    select * from orphan_monthly
)

select * from combined
order by recognized_month, addon_type
