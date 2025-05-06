from pyspark.sql import Column, SparkSession
from pyspark.sql.functions import col, length as spark_length, lit
import os
from pyspark_jobs.__init__ import get_schema_definition

def _filter_words_with_digits(x: Column)->Column:

    return x.rlike(r"^\D*$")  # Checks if the entire string contains no digits


def _words_length_filter(x: Column) -> Column:

    return spark_length(x)>=configuration["min_token_length"]

def _get_internal_field(struct: Column) -> Column:

    return struct.getField("result")

if __name__ == "__main__":

    configuration =({
        "min_token_length": 3,
        "number_messages_to_take": 10000000
        })

    spark = (SparkSession.builder.getOrCreate())

    # get script dir path
    dir_path = os.path.dirname(os.path.realpath(__file__))
    all_ingested_messages_path = os.path.join(dir_path, "../../../dialogs/")
    labelled_data_path = os.path.join(dir_path, "../../../labelled_messages_for_training")
    all_messages_of_interest_marked_with_an_undefined_label_path = os.path.join(
        dir_path, "../../../messages_for_training"
    )


    all_messages_df =(spark
          .read
          .option("multiline", "true")
     .option("recursiveFileLookup", "true")
     .schema(get_schema_definition())
     .json(all_ingested_messages_path))

    (all_messages_df
     .select("dialog", "user.id", "message_date", "message_text")
     .withColumnRenamed("id", "user_id")
     .filter(col("user_id") == "553068238")
     .limit(configuration["number_messages_to_take"])
     .withColumn("category",lit("undefined"))
     .write
     .mode("overwrite")
     .json(all_messages_of_interest_marked_with_an_undefined_label_path))

    messages_of_interest_df = spark.read.json(all_messages_of_interest_marked_with_an_undefined_label_path)

    df_labelled = spark.read.json(labelled_data_path).filter(~col("category").isin("undefined"))

    count_of_all_messages = all_messages_df.count()

    count_of_messages_of_interest_df = messages_of_interest_df.count()

    count_of_labelled_messages = df_labelled.count()


    all_messages_except_labelled = (messages_of_interest_df.drop("category")
     .subtract(df_labelled.drop("category"))
     .withColumn("category",lit("undefined"))
     .orderBy(col("message_date").desc()))

    count_of_all_messages_except_labelled = all_messages_except_labelled.count()

    (all_messages_except_labelled
     .write
     .mode("append")
     .partitionBy("category")
     .json(labelled_data_path))

    # print out counts
    print(f"Total messages count: {count_of_all_messages}")
    print(f"Total messages of interest count: {count_of_messages_of_interest_df}")
    print(f"Total labelled messages count: {count_of_labelled_messages}")
    print(f"Total undefined messages count: {count_of_all_messages_except_labelled}")

    spark.stop()