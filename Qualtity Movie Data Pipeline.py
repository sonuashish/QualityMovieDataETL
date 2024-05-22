import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
import concurrent.futures
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node S3 Data Source Catalog
S3DataSourceCatalog_node1716379220926 = glueContext.create_dynamic_frame.from_catalog(database="movies_data_catalog", table_name="quality_movie_data_analysis_by_ratings", transformation_ctx="S3DataSourceCatalog_node1716379220926")

# Script generated for node Data Quality Checks
DataQualityChecks_node1716379252965_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        IsComplete "imdb_rating",
            ColumnValues "imdb_rating" between 8.5 and 10.3
    ]
"""

DataQualityChecks_node1716379252965 = EvaluateDataQuality().process_rows(frame=S3DataSourceCatalog_node1716379220926, ruleset=DataQualityChecks_node1716379252965_ruleset, publishing_options={"dataQualityEvaluationContext": "DataQualityChecks_node1716379252965", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1716379278817 = SelectFromCollection.apply(dfc=DataQualityChecks_node1716379252965, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1716379278817")

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1716379280807 = SelectFromCollection.apply(dfc=DataQualityChecks_node1716379252965, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1716379280807")

# Script generated for node Conditional Router
ConditionalRouter_node1716379652442 = threadedRoute(glueContext,
  source_DyF = rowLevelOutcomes_node1716379280807,
  group_filters = [GroupFilter(name = "Failed-Records", filters = lambda row: (bool(re.match("Failed", row["DataQualityEvaluationResult"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("Failed", row["DataQualityEvaluationResult"])))))])

# Script generated for node default_group
default_group_node1716379652772 = SelectFromCollection.apply(dfc=ConditionalRouter_node1716379652442, key="default_group", transformation_ctx="default_group_node1716379652772")

# Script generated for node Failed-Records
FailedRecords_node1716379652828 = SelectFromCollection.apply(dfc=ConditionalRouter_node1716379652442, key="Failed-Records", transformation_ctx="FailedRecords_node1716379652828")

# Script generated for node Columns-Dropped
ColumnsDropped_node1716382050538 = ApplyMapping.apply(frame=default_group_node1716379652772, mappings=[("overview", "string", "overview", "string"), ("gross", "string", "gross", "string"), ("director", "string", "director", "string"), ("certificate", "string", "certificate", "string"), ("star4", "string", "star4", "string"), ("runtime", "string", "runtime", "string"), ("star2", "string", "star2", "string"), ("star3", "string", "star3", "string"), ("no_of_votes", "long", "no_of_votes", "int"), ("series_title", "string", "series_title", "string"), ("meta_score", "long", "meta_score", "int"), ("star1", "string", "star1", "string"), ("genre", "string", "genre", "string"), ("released_year", "string", "released_year", "string"), ("poster_link", "string", "poster_link", "string"), ("imdb_rating", "double", "imdb_rating", "decimal")], transformation_ctx="ColumnsDropped_node1716382050538")

# Script generated for node Rule Outcome
RuleOutcome_node1716379476177 = glueContext.write_dynamic_frame.from_options(frame=ruleOutcomes_node1716379278817, connection_type="s3", format="json", connection_options={"path": "s3://quality-movie-data-analysis-by-ratings/rule_outcome/", "partitionKeys": []}, transformation_ctx="RuleOutcome_node1716379476177")

# Script generated for node Failed Records Data in S3
FailedRecordsDatainS3_node1716381867257 = glueContext.write_dynamic_frame.from_options(frame=FailedRecords_node1716379652828, connection_type="s3", format="json", connection_options={"path": "s3://quality-movie-data-analysis-by-ratings/bad_records/", "partitionKeys": []}, transformation_ctx="FailedRecordsDatainS3_node1716381867257")

# Script generated for node Target Table in Catalog
TargetTableinCatalog_node1716382315346 = glueContext.write_dynamic_frame.from_catalog(frame=ColumnsDropped_node1716382050538, database="movies_data_catalog", table_name="movies-ratingdev_movies_imdb_movies_rating", redshift_tmp_dir="s3://temp-bucket-movie-data",additional_options={"aws_iam_role": "arn:aws:iam::058264560402:role/Redshift-s3-glue-role"}, transformation_ctx="TargetTableinCatalog_node1716382315346")

job.commit()