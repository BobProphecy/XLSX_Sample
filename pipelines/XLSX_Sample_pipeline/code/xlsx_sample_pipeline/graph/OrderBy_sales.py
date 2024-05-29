from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xlsx_sample_pipeline.config.ConfigStore import *
from xlsx_sample_pipeline.udfs.UDFs import *

def OrderBy_sales(spark: SparkSession, agg_by_customer: DataFrame) -> DataFrame:
    return agg_by_customer.orderBy(col("sales_totals").desc())
