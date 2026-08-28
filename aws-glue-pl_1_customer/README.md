# AWS Glue Job: pl_1_customer

Converted from Azure Data Factory ARM template.

## Directory Structure

```
pl_1_customer/
├── scripts/          # Glue ETL PySpark scripts
├── data/             # Data files (CSV, JSON, etc.)
├── iam/              # IAM trust and permission policies
├── cloudformation/   # CloudFormation deployment template
└── README.md         # This file
```

## Prerequisites

1. AWS CLI configured with appropriate credentials
2. S3 bucket created: `your-glue-bucket`
3. AWS Glue service available in your region

## Deployment Steps

### 1. Create S3 Bucket (if not exists)
```bash
aws s3 mb s3://your-glue-bucket
```

### 2. Upload Scripts to S3
```bash
aws s3 cp scripts/ s3://your-glue-bucket/scripts/ --recursive
```

### 3. Create IAM Role
```bash
aws iam create-role \
  --role-name pl_1_customer-glue-role \
  --assume-role-policy-document file://iam/glue_trust_policy.json

aws iam put-role-policy \
  --role-name pl_1_customer-glue-role \
  --policy-name pl_1_customer-glue-permissions \
  --policy-document file://iam/glue_permissions_policy.json
```

### 4. Deploy via CloudFormation
```bash
aws cloudformation deploy \
  --template-file cloudformation/pl_1_customer-glue-job.yaml \
  --stack-name pl_1_customer-stack \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    Environment=dev \
    S3BucketName=your-glue-bucket
```

### 5. Run the Glue Job
```bash
aws glue start-job-run --job-name pl_1_customer-dev
```

## Job Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| Environment | dev | Deployment environment |
| S3BucketName | your-glue-bucket | S3 bucket for scripts and data |
| NumberOfWorkers | 2 | Number of Glue workers |
| WorkerType | G.1X | Worker instance type |

## Worker Configuration

- **Glue Version:** 4.0 (Spark 3.3, Python 3.10)
- **Worker Type:** G.1X (4 vCPU, 16 GB memory per worker)
- **Number of Workers:** 2 (configurable)
- **Timeout:** 120 minutes
- **Max Retries:** 1

## Notes

- Source/sink paths in scripts use placeholder values - update to match your S3 layout.
- For JDBC connections, store credentials in AWS Secrets Manager.
- Enable CloudWatch logging for monitoring (already configured in CloudFormation).
