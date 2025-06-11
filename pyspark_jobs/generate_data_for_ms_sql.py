import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, to_date, to_json, collect_list, struct, isnotnull
from delta import *
from __init__ import get_raw_schema_definition

if __name__ == "__main__":

    if not os.path.isdir(f"../dialogs/"):
        print("There are no messages for creating the table. Please, park the messages first under the `dialogs` folder!")
        sys.exit(1)

    builder = (SparkSession
        .builder
        .appName("Creating-single-1-line-json-out-of-messages"))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df = (spark
          .read
          .option("multiline","true")
          .option("recursiveFileLookup", "true")
          .schema(get_raw_schema_definition())
          .json("../dialogs/")
          .filter(isnotnull(col("user")))
          .withColumn("path",input_file_name())
          .withColumn("message_date",to_date(col("message_date")))
          .withColumn("first_name",col("user").getItem("first_name"))
          .orderBy(col("message_date").desc_nulls_last()))

    df_json_rows = (df.select("path","message_date","message_text","first_name"))

    # 2. Зібрати всі JSON-рядки в єдиний масив JSON-рядків
    # collect_list збирає всі значення стовпця 'json_string' в один список
    # Це призведе до DataFrame з одним рядком і однією колонкою, що містить масив JSON-рядків
    final_json_array_df = df_json_rows.agg(to_json(collect_list(struct("*"))).alias("json_array_str"))


    #json_array_df.show(truncate=False)

    single_json_output = final_json_array_df.select("json_array_str").collect()[0][0]

    output_path = "../ms_sql_data/"

    with open(f"{output_path}single_dialogs_array.json", "w", encoding="utf-16") as f:
        f.write(single_json_output)


    users = df.select("user.id",
                      "user.is_self",
                      "user.access_hash",
                      "user.first_name",
                      "user.last_name",
                      "user.stories_unavailable",
                      "user.phone").distinct()

    users.printSchema()

    users.show(truncate=False)

    spark.stop()
