# aws-glue-pipeline1-sink-to-snowflake-job

Glue job **`adf-pipeline1-sink-to-snowflake`**, migrated from Azure Data Factory pipeline **`pipeline1_sink_to_snowflake`**.

Read customers CSV/Delimited from Azure Blob (migrated to S3), filter CUSTOMER_ID == 'C0011', and write to Snowflake via JDBC using credentials from Secrets Manager.

- Job type: `glue_pyspark`
- Converted by: `openai-gpt-5-mini` via Snowflake Cortex
- Validation: Passed syntax and structure validation.

## Contents

```
aws-glue-pipeline1-sink-to-snowflake-job/
├── scripts/df_pipeline1_sink_to_snowflake.py
├── data/
├── iam/pipeline1-sink-to-snowflake-least-privilege-policy.json
├── cloudformation/pipeline1-sink-to-snowflake-glue-job.yaml
└── README.md
```

## Step 1 - Create the S3 bucket

The bucket is created once by the shared core stack, not per job. If you have not
deployed it yet:

```bash
aws cloudformation deploy \
  --stack-name adf-glue-migration-core \
  --template-file ../_shared/cloudformation/00-core-bucket-and-role.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides ProjectBucketName=my-adf-glue-migration-bucket
```

## Step 2 - Load the script and data files

```bash
BUCKET=my-adf-glue-migration-bucket

aws s3 cp scripts/df_pipeline1_sink_to_snowflake.py s3://$BUCKET/scripts/df_pipeline1_sink_to_snowflake.py
aws s3 cp data/ s3://$BUCKET/data/pipeline1-sink-to-snowflake/ --recursive
```

Data files in this bundle:

No data file was seeded. Drop the file into `data/` and upload it in step 2.

## Step 3 - IAM role and permissions

The shared Glue service role is created by the core stack and imported here by
name, so no per-job role is required.

`iam/pipeline1-sink-to-snowflake-least-privilege-policy.json` is the least-privilege policy for
*this* job specifically. Replace `<BUCKET_NAME>`, `<AWS_REGION>`,
`<AWS_ACCOUNT_ID>` and `<SECRET_PREFIX>` if you attach it by hand. To let
CloudFormation attach it for you instead:

```bash
--parameter-overrides AttachLeastPrivilegePolicy=true
```

Secrets this job expects in AWS Secrets Manager:

- `glue/adf/snowflake` - Holds Snowflake credentials: user, password, account identifier, warehouse, role, database, host. Used by Glue job to connect to Snowflake via JDBC.
- `glue/adf/blobstorage` - Optional: if any connector requires storage account credentials during migration; originally Azure Blob connection string. Prefer copying files into S3 and then removing need for this secret.

## Step 4 - Configure job parameters

| Parameter | Purpose | Default |
|---|---|---|
| `--SOURCE_PATH` | S3 prefix containing the source customer files (e.g. s3://bucket/path/). | `s3://BUCKET/prefix/` |
| `--SINK_TABLE` | Snowflake target table name (database.schema.table). | `DB.SCHEMA.CUSTOMERS` |
| `--SNOWFLAKE_SECRET_ARN` | ARN of the Secrets Manager secret containing Snowflake credentials (user, password, account, role, warehouse, database, host). | `arn:aws:secretsmanager:region:acct:secret:glue/adf/snowflake` |
| `--TEMP_DIR` | S3 temp dir for Glue/Snowflake staging (if required). | `s3://BUCKET/tmp/` |

Each row above is also a CloudFormation parameter, so it can be overridden at
deploy time without editing the template.

## Step 5 - Worker configuration

| Setting | Value |
|---|---|
| Glue version | `4.0` |
| Worker type | `G.1X` |
| Number of workers | `2` |
| Timeout (minutes) | `60` |
| Max retries | `0` |
| Max concurrent runs | `1` |

## Deploy the job

```bash
aws cloudformation deploy \
  --stack-name pipeline1-sink-to-snowflake-glue-job \
  --template-file cloudformation/pipeline1-sink-to-snowflake-glue-job.yaml \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides CoreStackName=adf-glue-migration-core

aws glue start-job-run --job-name adf-pipeline1-sink-to-snowflake
```

## Sources and sinks

**Sources**

- `ds_customers_src` - s3_csv - Read from S3 path provided via --SOURCE_PATH (S3 location containing customer delimited files). Use header inference/explicit schema with columns CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE, STATE, CREATED_DATE, SOURCE_SYSTEM. Configure ignoreMissingFiles=false.

**Sinks**

- `Snowflake_Sink` - other - Write to Snowflake using Snowflake JDBC via AWS Secrets Manager for credentials. Use Snowflake staging via JDBC connector (copy via Snowflake PUT is optional). Target table provided via --SINK_TABLE. Use insert-only semantics (no updates/upserts).

## Confirm before production

- Confirm source files are copied from Azure Blob to S3 and path provided in --SOURCE_PATH before running.
- Validate Snowflake JDBC connectivity details and network egress (VPC, NAT) — secret must contain correct account/host and warehouse info.
- Confirm data types mapping for CREATED_DATE (date) and any timezone conversions; Glue schema drift settings may differ from ADF.
- Ensure write semantics: original dataflow was insert-only; migration must not perform updates/upserts. Confirm target table permissions and existence.
- If large volumes exist, current 2 workers may be insufficient; monitor job and scale workers if needed.

---
Generated by **ADF_to_AWS_Glue_App**. Review all generated code and IAM policy
before deploying to a production account.
