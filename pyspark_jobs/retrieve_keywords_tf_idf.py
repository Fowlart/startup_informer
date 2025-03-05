from pyspark.sql import SparkSession
from delta import *

if __name__ == "__main__":

    builder = (SparkSession
        .builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df = (spark
          .read
          .format("delta")
          .load("./../message_table")
          .select("dialog","user.id","message_date","message_text")
          .withColumnRenamed("id","user_id"))

    #todo: add spark code to retrieve key-words from messages with the usage tf/idf algorithm here
    df.show()

    spark.stop()
