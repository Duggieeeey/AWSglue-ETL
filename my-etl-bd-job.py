import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs
import re


def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(
                ctx,
                field.dataType,
                new_path + field.name,
                output,
                nullStringSet,
                nullIntegerSet,
                frame,
            )
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(
                ctx,
                schema.elementType,
                path,
                output,
                nullStringSet,
                nullIntegerSet,
                frame,
            )
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split(".")[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set(
                    [
                        item.strip() if isinstance(item, str) else item
                        for item in distinct_
                    ]
                )
            elif isinstance(distinct_, str):
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif (
            isinstance(schema, IntegerType)
            or isinstance(schema, LongType)
            or isinstance(schema, DoubleType)
        ):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output


def drop_nulls(
    glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx
) -> DynamicFrame:
    nullColumns = _find_null_fields(
        frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame
    )
    return DropFields.apply(
        frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx
    )


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1701827577807 = glueContext.create_dynamic_frame.from_catalog(
    database="my-database-bd",
    table_name="my_raw_data_bucket_bd",
    transformation_ctx="AWSGlueDataCatalog_node1701827577807",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1701827608306 = DynamicFrame.fromDF(
    AWSGlueDataCatalog_node1701827577807.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1701827608306",
)

# Script generated for node Drop Null Fields
DropNullFields_node1701827612376 = drop_nulls(
    glueContext,
    frame=DropDuplicates_node1701827608306,
    nullStringSet={""},
    nullIntegerSet={},
    transformation_ctx="DropNullFields_node1701827612376",
)

# Script generated for node Filter
Filter_node1701827645011 = Filter.apply(
    frame=DropNullFields_node1701827612376,
    f=lambda row: (
        row["stars"] >= 4
        and row["reviews"] >= 10000
        and row["boughtinlastmonth"] >= 10000
    ),
    transformation_ctx="Filter_node1701827645011",
)

# Script generated for node Glued Data
GluedData_node1701828798877 = glueContext.getSink(
    path="s3://my-result-data-bucket-bd",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="GluedData_node1701828798877",
)
GluedData_node1701828798877.setCatalogInfo(
    catalogDatabase="my-database-bd", catalogTableName="my_result_data_bucket_bd"
)
GluedData_node1701828798877.setFormat("csv")
GluedData_node1701828798877.writeFrame(Filter_node1701827645011)
job.commit()
