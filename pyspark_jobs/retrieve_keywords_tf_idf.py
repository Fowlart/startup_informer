from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import udf, col, filter, regexp_extract, length, lit, array
from pyspark.sql.types import ArrayType,StringType
from pyspark.ml.feature import IDF, CountVectorizer
from delta import *
from azure_ai_utils import _analyze_text, extract_key_phrases


@udf(returnType=ArrayType(StringType()))
def extract_tokens_udf(input_text: str) -> list[str]:
    """
    Extracts tokens from the input text using the specified analyzer.

    Args:
        input_text: The text to analyze.

    Returns:
        A list of tokens extracted from the text.
    """
    tokens_obj_list: list[dict[str, str]] = _analyze_text(
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

        if tfidf_score >= configuration["min_tf_idf_keyword_score"]:
            word_tfidf_pairs.append((tfidf_score, vocabulary[i]))

    sorted_pairs = sorted(word_tfidf_pairs, key=lambda x: x[0], reverse=True)

    top_keywords = [pair[1] for pair in sorted_pairs[:10]]

    return top_keywords

@udf(returnType=ArrayType(StringType()))
def extract_keywords_with_azure_udf(input_text: str) ->list[str]:
    return extract_key_phrases(text=input_text, language="uk")

def _filter_words_with_digits(x: col)->Column:
    return regexp_extract(x, r"\d", 0) == ""


def _words_length_filter(x: col) -> Column:
    return length(x)>=configuration["min_token_length"]

if __name__ == "__main__":
    configuration =({
        "min_tf_idf_keyword_score": 3,
        "min_token_length": 3,
        "min_df": 1,
        "min_tf": 1,
        "number_messages_to_take": 500000
        })

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
          .limit(configuration["number_messages_to_take"])
          .withColumn("tokens",extract_tokens_udf(col("message_text")))
          .withColumn("tokens", filter(col("tokens"), _words_length_filter))
          .withColumn("tokens",filter(col("tokens"),_filter_words_with_digits)))

    cv = CountVectorizer(inputCol="tokens", outputCol="tf_out", minTF=configuration["min_tf"], minDF=configuration["min_df"])

    cv_model = cv.fit(df)
    vectorized_tokens_df = cv_model.transform(df)

    idf = (IDF().setInputCol("tf_out").setOutputCol("idf_out"))

    df_with_idf_info = (idf
     .fit(vectorized_tokens_df)
     .transform(vectorized_tokens_df)
     .select("user_id",
             "message_date",
             "message_text",
             "tokens",
             "idf_out"
             ))

    dictionary = cv_model.vocabulary

    ddf_with_keywords = (df_with_idf_info
                         .withColumn("dictionary",array(*[lit(w) for w in dictionary]))
                         .withColumn("keywords_tf_idf", extract_keywords_udf(col("idf_out"), col("dictionary")))
                         .withColumn("keywords_azure",extract_keywords_with_azure_udf(col("message_text"))))

    # write an intermediate steps to the disk for debug and analysis
    (ddf_with_keywords
     .select(["user_id", "message_date","tokens","keywords_tf_idf","keywords_azure"])
     .write
     .json(path="./../key_words_extraction/df_with_keywords/", mode="overwrite"))

    df.write.json(path="./../key_words_extraction/df_with_tokens/", mode="overwrite")

    spark.stop()