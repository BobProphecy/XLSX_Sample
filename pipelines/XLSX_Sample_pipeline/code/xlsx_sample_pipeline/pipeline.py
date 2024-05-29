from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from xlsx_sample_pipeline.config.ConfigStore import *
from xlsx_sample_pipeline.udfs.UDFs import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

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
