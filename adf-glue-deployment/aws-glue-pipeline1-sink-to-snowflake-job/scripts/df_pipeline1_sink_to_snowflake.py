""""
Source ADF pipeline: pipeline1_sink_to_snowflake
"""

import sys
import json
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType

# Configure logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

# Secrets cache
_secrets_cache = {}

def get_secret(secret_name, region_name):
    """
    Retrieve a secret from AWS Secrets Manager with caching.
    Expected JSON structure:
      {
        "user": "<user>",
        "password": "<password>",
        "role": "<snowflake-role>",
        "host": "<optional-host or full account region host>"
      }
    """
    cache_key = f"{region_name}:{secret_name}"
    if cache_key in _secrets_cache:
        return _secrets_cache[cache_key]

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
            secret_dict = json.loads(secret)
        else:
            # If secret is binary, decode
            secret_dict = json.loads(get_secret_value_response["SecretBinary"])
        _secrets_cache[cache_key] = secret_dict
        return secret_dict
    except ClientError as e:
        logger.error("Failed to retrieve secret %s: %s", secret_name, e)
        raise

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_PATH",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_TABLE",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_WAREHOUSE",
        "FILTER_CUSTOMER_ID",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

try:
    # Helper: Determine region for Secrets Manager from session or Glue
    session = boto3.session.Session()
    region_name = session.region_name or spark.sparkContext._jsc.hadoopConfiguration().get("fs.s3a.awsRegion", "us-east-1")

    # Retrieve Snowflake credentials and role from Secrets Manager
    secret_name = "glue/adf/snowflake"
    logger.info("Retrieving Snowflake secret: %s", secret_name)
    sf_secret = get_secret(secret_name, region_name)
    sf_user = sf_secret.get("user")
    sf_password = sf_secret.get("password")
    sf_role = sf_secret.get("role")
    sf_host = sf_secret.get("host")  # optional; if not provided, we rely on account locator + region defaults

    if not sf_user or not sf_password:
        raise ValueError("Snowflake secret is missing required keys: user/password")

    # Activity: Data flow1 - ExecuteDataFlow
    # Section: srcCustomers - Read S3 CSV with explicit schema
    logger.info("Reading source customers from: %s", args["SOURCE_PATH"])

    schema = StructType(
        [
            StructField("CUSTOMER_ID", StringType(), True),
            StructField("CUSTOMER_NAME", StringType(), True),
            StructField("EMAIL", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("CREATED_DATE", StringType(), True),  # read as string first, will cast to date
            StructField("SOURCE_SYSTEM", StringType(), True),
        ]
    )

    df_src = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)
        .schema(schema)
        .load(args["SOURCE_PATH"])
    )

    row_count_src = df_src.count()
    logger.info("Source rows read: %d", row_count_src)

    # Transformation: filterstate - Filter by CUSTOMER_ID == FILTER_CUSTOMER_ID and cast date
    filter_id = args["FILTER_CUSTOMER_ID"]
    logger.info("Applying filter on CUSTOMER_ID == %s", filter_id)
    df_filtered = (
        df_src.filter(F.col("CUSTOMER_ID") == F.lit(filter_id))
        .withColumn("CREATED_DATE", F.to_date(F.col("CREATED_DATE")))
    )

    filtered_count = df_filtered.count()
    logger.info("Rows after filter: %d", filtered_count)

    # Section: sinkCurated - Write to Snowflake via Spark-Snowflake connector
    # Build Snowflake connector options
    # sfURL typically looks like: '<account>.snowflakecomputing.com'
    # If host is provided in secret, use it; otherwise, construct from account id
    account = args["SNOWFLAKE_ACCOUNT"]
    sf_url = sf_host if sf_host else f"{account}.snowflakecomputing.com"

    sf_options = {
        "sfURL": sf_url,
        "sfUser": sf_user,
        "sfPassword": sf_password,
        "sfDatabase": args["SNOWFLAKE_DATABASE"],
        "sfSchema": args["SNOWFLAKE_SCHEMA"],
        "sfWarehouse": args["SNOWFLAKE_WAREHOUSE"],
        "sfRole": sf_role if sf_role else "",
        "sfAccount": account,
    }

    # Ensure columns align and types as expected by sink (cast CREATED_DATE to date)
    # Already cast above; ensure ordering if needed
    write_df = df_filtered.select(
        "CUSTOMER_ID",
        "CUSTOMER_NAME",
        "EMAIL",
        "PHONE",
        "STATE",
        "CREATED_DATE",
        "SOURCE_SYSTEM",
    )

    logger.info(
        "Writing %d rows to Snowflake %s.%s.%s (mode=append)",
        filtered_count,
        args["SNOWFLAKE_DATABASE"],
        args["SNOWFLAKE_SCHEMA"],
        args["SNOWFLAKE_TABLE"],
    )

    # Write using Snowflake connector
    (
        write_df.write.format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .option("dbtable", args["SNOWFLAKE_TABLE"])
        .mode("append")
        .save()
    )

    logger.info("Write to Snowflake completed successfully.")

    # ORCHESTRATION: None required for this pipeline. This Glue job handles a single unit of work based on parameters.

except Exception as e:
    logger.exception("Job failed with exception: %s", e)
    raise
finally:
    job.commit()"