import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1773717465478 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleaned", table_name="raw_statistics", transformation_ctx="AWSGlueDataCatalog_node1773717465478")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1773717429490 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleaned", table_name="cleaned_statistics_reference_data", transformation_ctx="AWSGlueDataCatalog_node1773717429490")

# Script generated for node Join
Join_node1773717529000 = Join.apply(frame1=AWSGlueDataCatalog_node1773717465478, frame2=AWSGlueDataCatalog_node1773717429490, keys1=["category_id"], keys2=["id"], transformation_ctx="Join_node1773717529000")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1773717529000, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1773717354124", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1773718541721 = glueContext.getSink(path="s3://de-yt-analytics-useast2ohio-dev-analyticsbucket", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1773718541721")
AmazonS3_node1773718541721.setCatalogInfo(catalogDatabase="db_youtube_analytics",catalogTableName="final_analytics")
AmazonS3_node1773718541721.setFormat("glueparquet", compression="snappy")
AmazonS3_node1773718541721.writeFrame(Join_node1773717529000)
job.commit()