# Healthcare Data Lake with Governance 🏥

A HIPAA-compliant healthcare data lake built on AWS using 
the Medallion Architecture (Bronze/Silver/Gold).

## Architecture
Raw CSV → S3 Bronze → Glue Crawler → Glue ETL (PySpark) 
→ S3 Silver → Glue ETL → S3 Gold → Athena → Tableau

## Tech Stack
- **AWS Glue (PySpark)** — ETL transformation jobs
- **Amazon S3** — Medallion Architecture (Bronze/Silver/Gold)
- **AWS Lake Formation** — Column-level security & governance
- **AWS IAM** — Role-based access control
- **Amazon Athena** — Serverless SQL querying
- **CloudWatch** — Pipeline monitoring & alerting
- **Tableau** — Interactive analytics dashboard

## Security & Compliance
- HIPAA compliant — patient names removed in Silver layer
- Column-level security via Lake Formation:
  - Data Scientists: age, medical_condition, billing_amount, 
    test_results only
  - Doctors: all columns except billing and insurance info
- CloudWatch alerts for pipeline failures

## Medallion Architecture
- **Bronze** — Raw CSV files as ingested from hospital system
- **Silver** — Cleaned, anonymised Parquet files
- **Gold** — Pre-aggregated business metrics

## Key Insights
- Emergency admissions cost more than elective procedures
- Test results distribution shows majority normal outcomes
- Pipeline reliably processes 55,000+ patient records

## Dashboard
📊 Live Tableau Dashboard: https://public.tableau.com/app/profile/noufil.iqbal/viz/Healthcare_Analytics_17747026422710/HealthcareDataLakeAnalyticsDashboard

## GitHub
🔗 https://github.com/Noufil-Iqbal/healthcare-data-lake-aws
