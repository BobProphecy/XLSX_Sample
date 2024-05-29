from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from xlsx_sample_pipeline.config.ConfigStore import *
from xlsx_sample_pipeline.udfs.UDFs import *
from prophecy.utils import *
from xlsx_sample_pipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_snow_customers = snow_customers(spark)
    df_xlsx_orders = xlsx_orders(spark)
    df_snow_customers_xlsx_orders = snow_customers_xlsx_orders(spark, df_xlsx_orders, df_snow_customers)
    df_clean_up = clean_up(spark, df_snow_customers_xlsx_orders)
    df_agg_by_customer = agg_by_customer(spark, df_clean_up)
    df_OrderBy_sales = OrderBy_sales(spark, df_agg_by_customer)
    DBX_sales_rollup(spark, df_OrderBy_sales)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("XLSX_Sample_pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/XLSX_Sample_pipeline")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/XLSX_Sample_pipeline", config = Config)(pipeline)

if __name__ == "__main__":
    main()
