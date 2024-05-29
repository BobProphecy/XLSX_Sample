from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xlsx_sample_pipeline.config.ConfigStore import *
from xlsx_sample_pipeline.udfs.UDFs import *

def clean_up(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("CUSTOMER_ID"), 
        concat(col("FIRST_NAME"), lit(" "), col("LAST_NAME")).alias("full_name"), 
        col("order_id"), 
        col("amount")
    )
