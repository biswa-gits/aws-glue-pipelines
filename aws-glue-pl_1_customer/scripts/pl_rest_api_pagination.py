"""
AWS Glue ETL Job: pl_rest_api_pagination
Converted from Azure Data Factory pipeline.
"""
import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


# Initialize Glue context
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Configuration - update these for your environment
SOURCE_PATH = "s3://your-source-bucket/input/"
SINK_PATH = "s3://your-sink-bucket/output/"
JDBC_URL = "jdbc:sqlserver://your-server:1433;databaseName=your-db"
DB_USER = "admin"
DB_PASSWORD = "changeme"  # Use AWS Secrets Manager in production
BUCKET = "your-bucket"
PREFIX = "your-prefix/"
OBJECT_KEY = "your-key"


def main():

    # Activity: ACT_copy_product (Copy: RestSource -> DelimitedTextSink)
    source_df = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        connection_options={"paths": [SOURCE_PATH]},
        transformation_ctx="ACT_copy_product_source"
    )

    sink_df = glueContext.write_dynamic_frame.from_options(
        frame=source_df,
        connection_type="s3",
        format="parquet",
        connection_options={"path": SINK_PATH},
        transformation_ctx="ACT_copy_product_sink"
    )


main()
job.commit()
