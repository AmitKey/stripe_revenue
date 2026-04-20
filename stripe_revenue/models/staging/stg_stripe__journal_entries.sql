-- Thin rename of raw_journal_entries. One row per journal entry (charge-to-addon mapping).
-- Not unique on charge_id — a single charge can map to multiple addon types
-- (e.g. ch_DDD appears twice: once for payments, once for screening).
with source as (
    select * from {{ source('stripe_raw', 'raw_journal_entries') }}
),

renamed as (
    select
        journalentry_id,
        charge_id,
        dbtenant_id,
        addon_type
    from source
)

select * from renamed
