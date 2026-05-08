# Serverless Data Platform (AWS)

## Architecture — Fully Event-Driven, Zero Servers

```
S3 File Drop
    │ (PutObject event)
    ▼
┌─────────────────┐     ┌──────────────────────────────────────────┐
│  Lambda:        │     │  DynamoDB: pipeline_config                │
│  file_arrival   │────▶│  (format, transforms, DQ rules, etc.)    │
│  _trigger       │     └──────────────────────────────────────────┘
└────────┬────────┘
         │ StartExecution
         ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step Functions: data_pipeline                                    │
│                                                                   │
│  ResolveConfig → ValidateInput → Bronze → Silver → DQ Gate → Gold│
│       │              │              │        │         │       │  │
│   [Lambda]       [Lambda]       [Glue]   [Glue]  [Lambda]  [Glue]│
│                                                                   │
│  On Success → NotifySuccess [Lambda → Slack + DynamoDB]           │
│  On Failure → HandleFailure [Lambda → SNS → PagerDuty]           │
└──────────────────────────────────────────────────────────────────┘
```

## Pipelines (Config-Driven from DynamoDB)

| Pipeline | Source | Format | SLA | Classification |
|----------|--------|--------|-----|----------------|
| Bloomberg | Vendor file drop | CSV (pipe-delimited) | 15 min | MNPI |
| Refinitiv | Vendor file drop | CSV | 30 min | MNPI |
| Payroll | Internal SFTP → S3 | CSV | 60 min | Confidential |
| Trade Settlements | Partner API → S3 | JSON | 10 min | MNPI |

## Key Design Decisions

1. **Config-driven**: All pipeline behavior defined in DynamoDB, not code
2. **Event-triggered**: Files arrive → pipeline starts automatically (no scheduling)
3. **Quality gates**: DQ checks between Silver and Gold (blocks bad data)
4. **Zero provisioned capacity**: Glue serverless, Lambda, DynamoDB on-demand
5. **Full observability**: CloudWatch dashboard, execution logs, Slack alerts

## Cost: $4,180/month (for 4 pipelines processing ~500 files/day)

Compare to EMR equivalent: ~$25,000/month (6x more expensive)
