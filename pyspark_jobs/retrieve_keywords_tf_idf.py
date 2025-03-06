from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType,StringType
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
          .select("dialog","user.id","message_date","message_text")
          # take first for analysing
          .limit(20)
          .withColumnRenamed("id","user_id")
          .withColumn("tokens",extract_tokens_udf(col("message_text"))))

    # write an intermediate step to the disk for analysis
    df.write.json(path="./../key_words_extraction/debug_key_words_step_1/")

    # todo: use tf/idf

    spark.stop()
