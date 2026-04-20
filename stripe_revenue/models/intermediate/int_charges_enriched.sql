-- Charges joined to journal entries. One row per Stripe charge.
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
        string_agg(distinct dbtenant_id, ',') as dbtenant_ids,
        string_agg(distinct addon_type, ',')  as addon_types,
        count(*)                              as journal_entry_count
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
        j.dbtenant_ids,
        j.addon_types,
        j.journal_entry_count,
        -- ch_DDD has two journal entries (payments + screening); amount attribution is ambiguous
        coalesce(j.journal_entry_count > 1, false) as has_multiple_addon_types,
        j.charge_id is null                        as is_unmatched_charge
    from charges c
    left join journal_entries_per_charge j on c.charge_id = j.charge_id
)

select * from enriched
