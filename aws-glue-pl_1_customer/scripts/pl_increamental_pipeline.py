"""
AWS Glue ETL Job: pl_increamental_pipeline
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

    # Activity: ACT_LKP_Get_High_WaterMark_TimeStamp (Lookup)
    lookup_df = glueContext.create_dynamic_frame.from_catalog(
        database="default",
        table_name="lookup_table",
        transformation_ctx="ACT_LKP_Get_High_WaterMark_TimeStamp_lookup"
    )

    # Activity: ACT_LKP_Get_Last_Modified_TimeStamp (Lookup)
    lookup_df = glueContext.create_dynamic_frame.from_catalog(
        database="default",
        table_name="lookup_table",
        transformation_ctx="ACT_LKP_Get_Last_Modified_TimeStamp_lookup"
    )

    # Activity: ACT_Copy_Data (Copy: AzureSqlSource -> DelimitedTextSink)
    source_df = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        connection_options={"paths": [SOURCE_PATH]},
        transformation_ctx="ACT_Copy_Data_source"
    )

    sink_df = glueContext.write_dynamic_frame.from_options(
        frame=source_df,
        connection_type="s3",
        format="parquet",
        connection_options={"path": SINK_PATH},
        transformation_ctx="ACT_Copy_Data_sink"
    )

    # Activity: ACT_Update_High_WaterMark (Copy: AzureSqlSource -> DelimitedTextSink)
    source_df = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        connection_options={"paths": [SOURCE_PATH]},
        transformation_ctx="ACT_Update_High_WaterMark_source"
    )

    sink_df = glueContext.write_dynamic_frame.from_options(
        frame=source_df,
        connection_type="s3",
        format="parquet",
        connection_options={"path": SINK_PATH},
        transformation_ctx="ACT_Update_High_WaterMark_sink"
    )


main()
job.commit()
