import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, input_file_name, to_date
from delta import *

if __name__ == "__main__":

    if not os.path.isdir(f"../dialogs/"):
        print("There are no messages for creating the table. Please, park the messages first under the `dialogs` folder!")
        sys.exit(1)

    builder = (SparkSession
        .builder
        .appName("Creating-delta-tables-out-of-messages")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df = (spark
          .read
          .option("multiline","true")
          .option("recursiveFileLookup", "true")
          .json("../dialogs/")
          .withColumn("path",input_file_name())
          .withColumn("message_date",to_date(col("message_date")))
          .orderBy(col("message_date").desc_nulls_last()))

    # todo: rid of it
    df.show(truncate=False)

    (df
     .write
     .mode("overwrite")
     .option("overwriteSchema","True")
     .format("delta")
     .save("../dialogs_delta"))

    spark.stop()
