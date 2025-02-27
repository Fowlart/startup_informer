from pyspark.sql import SparkSession
from delta import *

if __name__ == "__main__":

    builder = (SparkSession
        .builder
        .appName("read-message-delta-table")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df = (spark
          .read
          .format("delta")
          .load("./../user_table"))

    df.show(truncate=False)

    spark.stop()
