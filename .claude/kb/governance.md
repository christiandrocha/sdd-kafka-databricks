# KB: Governance & PII — sdd-kafka-databricks patterns
# LGPD-relevant fields and Unity Catalog controls — NOT YET IMPLEMENTED
# This file documents what exists in the data + recommended UC mechanisms.
# Grep showed no masking/RLS/column-mask code anywhere in this repo as of v1.2.0 —
# treat everything below as a design reference for /design, not current behavior.

## Where real PII flows through this pipeline

| Field | Tables | Notes |
|---|---|---|
| `cpf` | `users_mongo`, `users_mssql`, `silver.users` (merge_key) | Brazilian individual taxpayer ID — direct identifier |
| `orders.user_key` | `orders` (all layers) | Carries the CPF value directly into the orders hub table — not a surrogate key |
| `cnpj` | `restaurants` | Brazilian company taxpayer ID — direct identifier for a business, lower sensitivity than CPF but still regulated |
| `email`, `phone_number`, `delivery_address` | `users_mongo` | Direct contact PII |
| `first_name`, `last_name`, `date_birth` (or `birthday`) | `users_mssql`, `drivers` | Direct identifiers / quasi-identifiers |
| `license_number` | `drivers` | Direct identifier, also sensitive (driving eligibility) |
| `gold.user_behavior` | reads `silver.users.cpf` directly into a Gold analytics table | PII propagates past Silver into an aggregate table consumed by BI |

`gold.user_behavior` is the one place PII crosses from an operational layer
(Silver) into an analytics layer (Gold) without aggregation removing the
identifier — `register_gold_user_behavior()` selects `users.cpf` straight through.
If this table ever gets broad read access (dashboards, ad-hoc SQL), that's the
first place a column mask would matter.

## Why this matters for this project specifically

Brazil's LGPD (Lei Geral de Proteção de Dados) treats CPF as personal data
requiring a lawful basis for processing, and as sensitive in combination with
other identifying fields. This project's data is synthetic, but the pipeline
*shape* — CPF as a join key flowing through Bronze→Silver→Gold unmasked — is the
same shape a real Uber-Eats-like system would have, so it's worth treating the
KB recommendations below as what a production hardening pass would need, not
hypothetical busywork.

## Unity Catalog mechanisms available (none currently used here)

### Column masking (dynamic, role-based)

```sql
CREATE OR REPLACE FUNCTION ubereats_prod.governance.mask_cpf(cpf STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('pii_readers') THEN cpf
  ELSE CONCAT('***.***.***-', RIGHT(cpf, 2))
END;

ALTER TABLE ubereats_prod.silver.users
  ALTER COLUMN cpf SET MASK ubereats_prod.governance.mask_cpf;
```

Same pattern would apply to `gold.user_behavior.cpf`, `drivers.license_number`,
`users_mongo.email`/`phone_number`/`delivery_address`.

### Row filters (restrict by attribute, not column)

```sql
CREATE OR REPLACE FUNCTION ubereats_prod.governance.restrict_by_city(city STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('admin') OR city = current_user_city();

ALTER TABLE ubereats_prod.gold.driver_performance
  SET ROW FILTER ubereats_prod.governance.restrict_by_city ON (city);
```

Less relevant here than column masking — this project's segmentation is by
domain/layer, not by tenant/region, so row filters are a lower priority than
masking the direct identifiers above.

### Tags + attribute-based access control (ABAC)

Unity Catalog tags (`SET TAG`) can mark columns as `pii_category = 'cpf'` /
`'contact_info'` and drive masking policies centrally instead of one `MASK`
function per column. Worth doing once more than 2-3 columns need the same
treatment — applying it now to `cpf` + `email` + `phone_number` + `license_number`
would already clear that bar.

### Lineage via system tables

`system.access.table_lineage` / `system.access.column_lineage` would show that
`cpf` flows `users_mongo`/`users_mssql` → `silver.users` → `gold.user_behavior`
without any manual lineage doc — useful for a future LGPD data-mapping exercise
("where does this identifier end up") without re-deriving it from
`pipelines/ubereats_pipeline.py` by hand.

## What NOT to do here

Don't add masking/row filters speculatively without a concrete consumer
(a real dashboard, a real external read access grant) — this is a 129k-row
architectural microcosm (per `CLAUDE.md`'s "Dataset framing"), not a production
system with actual third-party readers yet. Treat this file as the reference to
pull from *when* `/design` decides UC governance is in scope, not a checklist to
implement unprompted.

## Anti-patterns

| Never do | Why | Instead |
|---|---|---|
| Grant broad `SELECT` on `silver.users` or `gold.user_behavior` to a non-`pii_readers` group | `cpf` is unmasked in both today | Gate behind a UC column mask first, or restrict the grant to columns that exclude `cpf` |
| Treat `cnpj` as equivalent risk to `cpf` | CNPJ identifies a business, not a person — different LGPD basis | Don't apply the same masking policy/severity to both without a reason |
| Add a masking function per column ad hoc as new PII columns are found | Doesn't scale past a handful of columns, drifts from a single policy | Tag-based ABAC once 3+ columns need the same treatment |
