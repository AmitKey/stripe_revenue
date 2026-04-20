-- Thin rename of raw_stripe_charges. One row per Stripe charge (one payment from an end customer).
-- expected_transfer_amount is kept for reference only — it is set at charge creation time
-- and is less accurate than the actual transfer event that arrives later.
with source as (
    select * from {{ source('stripe_raw', 'raw_stripe_charges') }}
),

renamed as (
    select
        charge_id,
        stripe_customer_id,
        stripe_tenant_customer_id,
        charge_amount,
        charge_status,
        created_at                as charged_at,
        -- Set at charge-creation time; less accurate than activity transfer events
        journalentry_id           as journalentry_id_at_creation,
        expected_transfer_amount
    from source
)

select * from renamed
