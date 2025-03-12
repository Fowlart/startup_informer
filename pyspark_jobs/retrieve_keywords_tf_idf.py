from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import udf, col, filter, regexp_extract, length, explode, lit, array, collect_set
from pyspark.sql.types import ArrayType,StringType
from pyspark.ml.feature import HashingTF, IDF, CountVectorizer

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

# UDF to extract keywords
@udf(returnType=ArrayType(StringType()))
def extract_keywords_udf(tfidf_vector, vocabulary: list[str]):

    if tfidf_vector is None:
        return []

    word_tfidf_pairs = []

    for i, tfidf_score in enumerate(tfidf_vector):
        # configure this
        if tfidf_score > 2:
            word_tfidf_pairs.append((tfidf_score, vocabulary[i]))

    sorted_pairs = sorted(word_tfidf_pairs, key=lambda x: x[0], reverse=True)

    top_keywords = [pair[1] for pair in sorted_pairs[:10]]

    return top_keywords


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
           #.limit(30)

          .withColumn("tokens",extract_tokens_udf(col("message_text")))
          .withColumn("tokens", filter(col("tokens"), _words_length_filter))
          .withColumn("tokens",filter(col("tokens"),_filter_words_with_digits))
          )

    # spark tf/idf
    cv = CountVectorizer(inputCol="tokens", outputCol="tf_out")
    cv_model = cv.fit(df)
    df_tf = cv_model.transform(df)


    idf = (
        IDF()
        .setInputCol("tf_out")
        .setOutputCol("idf_out")
        .setMinDocFreq(2))

    df_with_idf_info = (idf
     .fit(df_tf)
     .transform(df_tf)
     .select("user_id",
             "message_date",
             "message_text",
             "tokens",
             "idf_out"
             ))

    # Apply the UDF
    dictionary = cv_model.vocabulary
    dict_df = (spark.sparkContext
               .parallelize(dictionary)
               .map(lambda x:(x,)).toDF(["terms"])
               .groupBy()
               .agg(collect_set("terms").alias("terms_array"))
               .select("terms_array"))

    dict_df.show()

    ddf_with_keywords = dict_df.crossJoin(df_with_idf_info).withColumn("keywords", extract_keywords_udf(col("idf_out"), col("terms_array")))

    # write an intermediate steps to the disk for debug and analysis
    ddf_with_keywords.select(["user_id", "message_date","tokens","keywords"]).write.json(path="./../key_words_extraction/result/", mode="overwrite")
    df.write.json(path="./../key_words_extraction/debug_key_words_step_1/", mode="overwrite")
    df_with_idf_info.write.json(path="./../key_words_extraction/debug_key_words_step_2/", mode="overwrite")

    spark.stop()