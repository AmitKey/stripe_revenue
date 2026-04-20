-- Charges joined to journal entries. One row per succeeded Stripe charge.
-- Non-succeeded charges (failed, pending) are excluded by the final WHERE clause.
-- Multi-addon charges (e.g. ch_DDD) are collapsed into a single row — addon types become
-- a comma-separated string and has_multiple_addon_types is set to true.
-- Charges with no journal entry are kept via LEFT JOIN and flagged as is_unmatched_charge.
with charges as (
    select * from {{ ref('stg_stripe__charges') }}
),

journal_entries as (
    select * from {{ ref('stg_stripe__journal_entries') }}
),

-- Aggregate journal entries per charge to handle multi-addon charges (e.g. ch_DDD)
journal_entries_per_charge as (
    select
        charge_id,
        array_to_string(list_sort(list_distinct(list(dbtenant_id))), ',') as dbtenant_ids,
        array_to_string(list_sort(list_distinct(list(addon_type))), ',')  as addon_types,
        count(*)                                                           as journal_entry_count,
        count(distinct addon_type)                                         as distinct_addon_type_count
    from journal_entries
    group by charge_id
),

enriched as (
    select
        c.charge_id,
        c.stripe_customer_id,
        c.stripe_tenant_customer_id,
        c.charge_amount,
        c.charge_status,
        c.charged_at,
        c.expected_transfer_amount,
        j.dbtenant_ids,
        j.addon_types,
        j.journal_entry_count,
        -- ch_DDD has two journal entries (payments + screening); amount attribution is ambiguous
        coalesce(j.distinct_addon_type_count > 1, false) as has_multiple_addon_types,
        j.charge_id is null                               as is_unmatched_charge
    from charges c
    left join journal_entries_per_charge j on c.charge_id = j.charge_id
)

select * from enriched
where charge_status = 'succeeded'
