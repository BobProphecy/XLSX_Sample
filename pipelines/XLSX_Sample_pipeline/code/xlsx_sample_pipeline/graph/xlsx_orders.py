from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xlsx_sample_pipeline.config.ConfigStore import *
from xlsx_sample_pipeline.udfs.UDFs import *

def xlsx_orders(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("excel")\
        .option("header", True)\
        .option("dataAddress", "'orders'!A1")\
        .load("dbfs:/FileStore/bobwelshmer/sample_data/hello_world.xlsx")
