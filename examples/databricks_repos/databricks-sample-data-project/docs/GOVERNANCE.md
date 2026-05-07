# Governance Controls

> Applied to 8 tables in `acme_prod` catalog

## Classification Tags

All tables are auto-tagged with:
```sql
ALTER TABLE {table} SET TAGS ('data_classification' = '{classification}');
ALTER TABLE {table} SET TAGS ('domain' = '{domain}', 'owner' = '{owner}');
```

## MNPI Column Masks
No MNPI columns detected in this migration.

## PII Column Masks

### payment_processing
- **Table:** `acme_prod.lending_bronze.payment_processing`
- **PII columns:** `account_number`
- **Mask:** SHA-256 hash for unauthorized users


## Access Control Groups

| Group | Access Level | Tables |
|-------|-------------|--------|
| `analytics_team` | Full access to `analytics_*` schemas | All analytics tables |
| `customer_team` | Full access to `customer_*` schemas | All customer tables |
| `finance_team` | Full access to `finance_*` schemas | All finance tables |
| `lending_team` | Full access to `lending_*` schemas | All lending tables |
| `risk_team` | Full access to `risk_*` schemas | All risk tables |
| `data_engineering` | All schemas (manage) | All |
| `analysts` | Silver + Gold (read only) | Governed views |
| `compliance` | Full access (audit) | All + audit logs |

## Audit

All access is logged to `acme_prod._governance.audit_log` with 7-year retention.
