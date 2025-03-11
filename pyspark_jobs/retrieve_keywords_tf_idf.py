from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import udf, col, filter, regexp_extract, length, collect_set, flatten, explode, collect_list, \
    size, array_distinct
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType,StringType
from pyspark.ml.feature import HashingTF, IDF


from delta import *
from azure_ai_utils import analyze_text


@udf(returnType=ArrayType(StringType()))
def extract_tokens_udf(input_text: str) -> list[str]:
    """
    Extracts tokens from the input text using the specified analyzer.

    Args:
        input_text: The text to analyze.

    Returns:
        A list of tokens extracted from the text.
    """
    tokens_obj_list: list[dict[str, str]] = analyze_text(
        input_text, analyzer_name="uk.microsoft"
    )

    return [token_obj["token"] for token_obj in tokens_obj_list if "token" in token_obj]

def _filter_words_with_digits(x: col)->Column:
    return regexp_extract(x, r"\d", 0) == ""

def _words_length_filter(x: col) -> Column:
    return length(x)>=3

if __name__ == "__main__":

    builder = (SparkSession
        .builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.udf.register("extract_tokens_udf",extract_tokens_udf)

    df = (spark
          .read
          .format("delta")
          .load("./../message_table")
          .select("dialog", "user.id", "message_date", "message_text")
          .withColumnRenamed("id","user_id")
          .filter(col("user_id") == "553068238")

           #todo: limit will speed up the process, remove after testing
           .limit(30)

          .withColumn("tokens",extract_tokens_udf(col("message_text")))
          .withColumn("tokens", filter(col("tokens"), _words_length_filter))
          .withColumn("tokens",filter(col("tokens"),_filter_words_with_digits))
          )

    window_definition = Window.rowsBetween(start=Window.unboundedPreceding,end=0)

    dictionary_df = (df
                     .withColumn("dict",array_distinct(flatten(collect_list(col("tokens")).over(window_definition))))
                     .withColumn("dict_size",size(col("dict")))
                     .orderBy(col("dict_size").desc())
                     .limit(1)
                     )

    dictionary = dictionary_df.take(1)[0]["dict"]

    print(f"The dictionary: {dictionary}")
    print(f"The dictionary length: {len(dictionary)}")

    # spark tf/idf
    tf = (
        HashingTF()
        .setInputCol("tokens")
        .setOutputCol("tf_out")
        #todo: count qty of unique tokens
        .setNumFeatures(len(dictionary)))

    idf = (
        IDF()
        .setInputCol("tf_out")
        .setOutputCol("idf_out")
        .setMinDocFreq(3))

    tf_transformed = tf.transform(df)

    df_with_idf_info = (idf
     .fit(tf_transformed)
     .transform(tf_transformed)
     .select("user_id",
             "message_date",
             "message_text",
             "tokens",
             "idf_out"
             ))

    # write an intermediate steps to the disk for analysis
    dictionary_df.write.json(path="./../key_words_extraction/debug_dictionary/", mode="overwrite")

    df.write.json(path="./../key_words_extraction/debug_key_words_step_1/", mode="overwrite")

    (df_with_idf_info.write.json(path="./../key_words_extraction/debug_key_words_step_2/", mode="overwrite"))

    spark.stop()