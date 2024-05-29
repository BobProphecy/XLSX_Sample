from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xlsx_sample_pipeline.config.ConfigStore import *
from xlsx_sample_pipeline.udfs.UDFs import *

def snow_customers_xlsx_orders(spark: SparkSession, xlsx_orders: DataFrame, snow_customers: DataFrame, ) -> DataFrame:
    return xlsx_orders\
        .alias("xlsx_orders")\
        .join(
          snow_customers.alias("snow_customers"),
          (col("xlsx_orders.customer_id") == col("snow_customers.CUSTOMER_ID")),
          "inner"
        )\
        .select(col("snow_customers.CUSTOMER_ID").alias("CUSTOMER_ID"), col("snow_customers.FIRST_NAME").alias("FIRST_NAME"), col("snow_customers.LAST_NAME").alias("LAST_NAME"), col("xlsx_orders.order_id").alias("order_id"), col("xlsx_orders.amount").alias("amount"))
