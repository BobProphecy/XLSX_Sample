from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xlsx_sample_pipeline.config.ConfigStore import *
from xlsx_sample_pipeline.udfs.UDFs import *

def agg_by_customer(spark: SparkSession, clean_up: DataFrame) -> DataFrame:
    df1 = clean_up.groupBy(col("CUSTOMER_ID"), col("full_name"))

    return df1.agg(count(col("order_id")).alias("order_count"), sum(col("amount")).alias("sales_totals"))
