import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_null_rows
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1701892383468 = glueContext.create_dynamic_frame.from_catalog(
    database="my-database-bd",
    table_name="my_raw_data_bucket_bd",
    transformation_ctx="AWSGlueDataCatalog_node1701892383468",
)

# Script generated for node Filter
Filter_node1701892398075 = Filter.apply(
    frame=AWSGlueDataCatalog_node1701892383468,
    f=lambda row: (
        row["stars"] >= 4
        and row["reviews"] >= 10000
        and row["boughtinlastmonth"] >= 10000
    ),
    transformation_ctx="Filter_node1701892398075",
)

# Script generated for node Drop Fields
DropFields_node1701892469134 = DropFields.apply(
    frame=Filter_node1701892398075,
    paths=["asin", "imgurl", "producturl", "listprice", "category_id"],
    transformation_ctx="DropFields_node1701892469134",
)

# Script generated for node Remove Null Rows
RemoveNullRows_node1701893067140 = DropFields_node1701892469134.gs_null_rows()

# Script generated for node Drop Duplicates
DropDuplicates_node1701893086966 = DynamicFrame.fromDF(
    RemoveNullRows_node1701893067140.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1701893086966",
)

# Script generated for node Amazon S3
AmazonS3_node1701924406965 = glueContext.getSink(
    path="s3://my-result-data-bucket-bd",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1701924406965",
)
AmazonS3_node1701924406965.setCatalogInfo(
    catalogDatabase="my-database-bd", catalogTableName="glued_data_bd"
)
AmazonS3_node1701924406965.setFormat("glueparquet")
AmazonS3_node1701924406965.writeFrame(DropDuplicates_node1701893086966)
job.commit()
