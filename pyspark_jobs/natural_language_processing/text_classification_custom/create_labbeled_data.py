from pyspark.sql import Column, SparkSession
from pyspark.sql.functions import col, regexp_extract, length as spark_length, lit
from pyspark_jobs.__init__ import get_schema_definition
import os

def _filter_words_with_digits(x: col)->Column:

    return regexp_extract(x, r"\d", 0) == ""

def _words_length_filter(x: col) -> Column:

    return spark_length(x)>=configuration["min_token_length"]

def _get_internal_field(struct: Column) -> Column:

    return struct.getField("result")

if __name__ == "__main__":

    configuration =({
        "min_token_length": 3,
        "number_messages_to_take": 10000000
        })

    spark = (SparkSession.builder.getOrCreate())

    dir_path = os.path.dirname(os.path.realpath(__file__))

    labelled_data_path = f"{dir_path}/../../../labelled_messages_for_training"
    unlabelled_data_path = f"{dir_path}/../../../messages_for_training"

    (spark
          .read
          .option("multiline", "true")
          .option("recursiveFileLookup", "true")
          .schema(get_schema_definition())
          .json(f"{dir_path}/../../../dialogs/")
          .select("dialog", "user.id", "message_date", "message_text")
          .withColumnRenamed("id", "user_id")
          .filter(col("user_id") == "553068238")
          .limit(configuration["number_messages_to_take"])
          .withColumn("category",lit("undefined"))
          .write
          .mode("overwrite")
          .json(unlabelled_data_path))

    df = spark.read.json(unlabelled_data_path)

    df_labelled = spark.read.json(labelled_data_path).filter(~col("category").isin("undefined"))

    (df.drop("category")
     .subtract(df_labelled.drop("category"))
     .withColumn("category",lit("undefined"))
     .write.mode("append").partitionBy("category").json(labelled_data_path))

    spark.stop()