from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import udf, col, regexp_extract, length as spark_length, lit, array, transform, filter as spark_filter
from pyspark.sql.types import ArrayType,StringType
from pyspark.ml.feature import IDF, CountVectorizer
from sparknlp import DocumentAssembler
from sparknlp.annotator import LemmatizerModel, Tokenizer
from pyspark_jobs.azure_ai_utils import extract_key_phrases
from pyspark_jobs.__init__ import get_schema_definition
import sparknlp


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

    top_keywords = [pair[1] for pair in sorted_pairs[:configuration["max_keywords_count"]]]

    return top_keywords

@udf(returnType=ArrayType(StringType()))
def extract_keywords_with_azure_udf(input_text: str) ->list[str]:

    return extract_key_phrases(text=input_text, language="uk")


def _filter_words_with_digits(x: col)->Column:

    return regexp_extract(x, r"\d", 0) == ""


def _words_length_filter(x: col) -> Column:

    return spark_length(x)>=configuration["min_token_length"]


def _get_internal_field(struct: Column) -> Column:

    return struct.getField("result")

if __name__ == "__main__":
    configuration =({
        "min_tf_idf_keyword_score": 3,
        "min_token_length": 4,
        "min_df": 1,
        "min_tf": 1,
        "max_keywords_count": 5,
        "number_messages_to_take": 10000000
        })

    spark = (sparknlp.start())

    df = (spark
          .read
          .option("multiline", "true")
          .option("recursiveFileLookup", "true")
          .schema(get_schema_definition())
          .json("./../../dialogs/")
          .select("dialog", "user.id", "message_date", "message_text")
          .withColumnRenamed("id","user_id")
          .filter( col("user_id") == "553068238" )
          .limit( configuration["number_messages_to_take"]) )

    documentAssembler = DocumentAssembler().setInputCol("message_text").setOutputCol("document")

    df = documentAssembler.transform(df)

    tokenizer = Tokenizer().setInputCols("document").setOutputCol("tokens_0")

    df = tokenizer.fit(df).transform(df)

    lemmatizer: LemmatizerModel = LemmatizerModel.pretrained("lemma", "uk").setInputCols(["tokens_0"]).setOutputCol("tokens_1")

    df = lemmatizer.transform(df)

    df = (df
          .withColumn("tokens",transform(col("tokens_1"),_get_internal_field))
          .withColumn("tokens", spark_filter(col("tokens"), _words_length_filter))
          .withColumn("tokens",spark_filter(col("tokens"),_filter_words_with_digits))
          )

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
     .json(path="./../../key_words_extraction/df_with_keywords/", mode="overwrite"))

    df.write.json(path="./../../key_words_extraction/df_with_tokens/", mode="overwrite")

    spark.stop()