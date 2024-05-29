from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xlsx_sample_pipeline.config.ConfigStore import *
from xlsx_sample_pipeline.udfs.UDFs import *

def snow_customers(spark: SparkSession) -> DataFrame:
    from pyspark.dbutils import DBUtils

    return spark.read\
        .format("snowflake")\
        .options(
          **{
            "sfUrl": "https://tu22760.ap-south-1.aws.snowflakecomputing.com/",
            "sfUser": "DEMOACCOUNT",
            "sfPassword": "{}".format(DBUtils(spark).secrets.get(scope = "bob_snow_demoaccount", key = "SnowSecret")),
            "sfDatabase": "BOBW",
            "sfSchema": "HELLOWORLD",
            "sfWarehouse": "",
            "sfRole": ""
          }
        )\
        .option("dbtable", "HW_CUSTOMERS")\
        .load()
