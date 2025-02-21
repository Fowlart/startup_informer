from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, input_file_name, to_date
from delta import *

if __name__ == "__main__":

    builder = (SparkSession
        .builder
        .appName("Read-message-delta-table")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df = (spark
          .read
          .format("delta")
          .option("recursiveFileLookup", "true")
          .load("../dialogs_delta"))

    df.show()

    spark.stop()
