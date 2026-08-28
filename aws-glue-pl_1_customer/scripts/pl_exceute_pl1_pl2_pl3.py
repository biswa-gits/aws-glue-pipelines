"""
AWS Glue ETL Job: pl_exceute_pl1_pl2_pl3
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

    # Activity: Execute Pipeline1 (Execute Pipeline -> Glue Workflow trigger)
    # Trigger child Glue job: PL_1_Customer
    client = boto3.client("glue")
    client.start_job_run(JobName="PL_1_Customer")

    # Activity: Execute Pipeline1_copy1 (Execute Pipeline -> Glue Workflow trigger)
    # Trigger child Glue job: PL_2_Product
    client = boto3.client("glue")
    client.start_job_run(JobName="PL_2_Product")

    # Activity: Execute Pipeline1_copy1_copy1 (Execute Pipeline -> Glue Workflow trigger)
    # Trigger child Glue job: PL_3_Address
    client = boto3.client("glue")
    client.start_job_run(JobName="PL_3_Address")


main()
job.commit()
