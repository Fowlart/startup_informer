import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, to_date, to_json, collect_list, struct, isnotnull
from delta import *
from pyspark.sql.types import BooleanType

from __init__ import get_raw_schema_definition

if __name__ == "__main__":

    if not os.path.isdir(f"../dialogs/"):
        print(
            "There are no messages for creating the table. Please, park the messages first under the `dialogs` folder!")
        sys.exit(1)

    builder = (SparkSession
               .builder
               .appName("Creating-single-1-line-json-out-of-messages"))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df = (spark
          .read
          .option("multiline", "true")
          .option("recursiveFileLookup", "true")
          .schema(get_raw_schema_definition())
          .json("../dialogs/")
          .filter(isnotnull(col("user")))
          .withColumn("path", input_file_name())
          .withColumn("message_date", to_date(col("message_date")))
          .withColumn("first_name", col("user").getItem("first_name"))
          .orderBy(col("message_date").desc_nulls_last()))

    messages_json_output = (
        df.select("path", "message_date", "message_text", "first_name")
          .agg(to_json(collect_list(struct("*"))).alias("json_array_str"))
          .select("json_array_str").collect()[0][0]
    )

    users_json_output = (df.select(col("user.id"),
                                   col("user.is_self").cast(BooleanType()).alias("is_self"),
                                   "user.access_hash",
                                   "user.first_name",
                                   "user.last_name",
                                   col("user.stories_unavailable").cast(BooleanType()).alias("stories_unavailable"),
                                   "user.phone").distinct()
    .agg(to_json(collect_list(struct("*"))).alias("json_array_str"))
    .select("json_array_str")
    .collect()[0][0])

    output_path = "../ms_sql_data/"

    with open(f"{output_path}single_dialogs_array.json", "w", encoding="utf-16") as f:
        f.write(messages_json_output)

    with open(f"{output_path}users_array.json", "w", encoding="utf-16") as f:
        f.write(users_json_output)

    spark.stop()
