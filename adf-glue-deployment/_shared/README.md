# _shared - deploy this first

Every per-job stack imports this stack's outputs, so it must exist before any
job stack is created.

## Contents

```
_shared/
├── cloudformation/00-core-bucket-and-role.yaml
├── iam/glue_trust_policy.json
├── iam/glue_service_role_permissions.json
└── README.md
```

- **`00-core-bucket-and-role.yaml`** - the S3 bucket (versioned, AES256, public
  access blocked, lifecycle rules on `temp/` and `spark-logs/`) and the shared
  Glue service role.
- **`glue_trust_policy.json`** - the role's trust relationship, allowing
  `glue.amazonaws.com` to assume it. Reference copy; the stack embeds it.
- **`glue_service_role_permissions.json`** - the role's baseline permissions as a
  reference document. Replace `<BUCKET_NAME>`, `<AWS_REGION>`,
  `<AWS_ACCOUNT_ID>` and `<SECRET_PREFIX>` if attaching manually.

## Deploy

```bash
aws cloudformation deploy \
  --stack-name adf-glue-migration-core \
  --template-file cloudformation/00-core-bucket-and-role.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
      ProjectBucketName=my-adf-glue-migration-bucket \
      SecretPrefix=glue/adf/
```

Outputs exported for the job stacks:

| Export | Used by job stacks for |
|---|---|
| `adf-glue-migration-core-BucketName` | script location, `--TempDir`, Spark event logs |
| `adf-glue-migration-core-GlueRoleArn` | the Glue job's execution role |
| `adf-glue-migration-core-GlueRoleName` | attaching optional per-job policies |

## Create the secrets

Each JDBC source needs a secret whose name starts with `glue/adf/`:

```bash
aws secretsmanager create-secret \
  --name glue/adf/sqlserver \
  --secret-string '{"username":"USER","password":"PASSWORD","host":"HOST","port":1433,"database":"DB"}'
```

## Job bundles importing this stack

- `aws-glue-pipeline1-sink-to-snowflake-job`
