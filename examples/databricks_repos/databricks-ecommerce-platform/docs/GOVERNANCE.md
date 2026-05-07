# Governance Controls

> Applied to 1 tables in `ecommerce_prod` catalog

## Classification Tags

All tables are auto-tagged with:
```sql
ALTER TABLE {table} SET TAGS ('data_classification' = '{classification}');
ALTER TABLE {table} SET TAGS ('domain' = '{domain}', 'owner' = '{owner}');
```

## MNPI Column Masks
No MNPI columns detected in this migration.

## PII Column Masks
No PII columns detected in this migration.

## Access Control Groups

| Group | Access Level | Tables |
|-------|-------------|--------|
| `analytics_team` | Full access to `analytics_*` schemas | All analytics tables |
| `data_engineering` | All schemas (manage) | All |
| `analysts` | Silver + Gold (read only) | Governed views |
| `compliance` | Full access (audit) | All + audit logs |

## Audit

All access is logged to `ecommerce_prod._governance.audit_log` with 7-year retention.
