from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import udf, col, regexp_extract, length as spark_length, transform, filter as spark_filter
from pyspark.sql.types import ArrayType, StringType, FloatType
from delta import *
from sparknlp import DocumentAssembler
from sparknlp.annotator import LemmatizerModel, Tokenizer, DistilBertForSequenceClassification

from azure_ai_utils import extract_key_phrases


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
        "min_token_length": 3,
        "min_df": 1,
        "min_tf": 2,
        "max_keywords_count": 5,
        "number_messages_to_take": 10000000
        })

    builder = (SparkSession.builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

    spark = configure_spark_with_delta_pip(builder,extra_packages=["com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.3"]).getOrCreate()

    df = (spark
          .read
          .format("delta")
          .load("./../message_table")
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

    sequenceClassifier = ( DistilBertForSequenceClassification.pretrained("mdistilbert_base_cased_ukrainian_toxicity","uk")
        .setInputCols(["document", "tokens_1"])
        .setOutputCol("class") )

    df = sequenceClassifier.transform(df)

    df = (df
          .withColumn("tokens",transform(col("tokens_1"),_get_internal_field))
          .withColumn("tokens", spark_filter(col("tokens"), _words_length_filter))
          .withColumn("tokens",spark_filter(col("tokens"),_filter_words_with_digits))
          )

    toxic_messages_df = (
        df.select(
            col("message_date"),
            col("message_text"),
            col("class")).filter(col("class").getItem(0).getField("metadata").getField("toxic").cast(FloatType())>0.98))

    (toxic_messages_df
     .write
     .json(path="./../text_classification/debug_1/", mode="overwrite"))

    msg_count = df.count()
    toxic_msg_count = toxic_messages_df.count()
    print(f"Total analyzed messages count: {msg_count}")
    print(f"Toxic messages count: {toxic_msg_count}")
    print(f"Toxicity level:  {toxic_msg_count/msg_count*100} %")

    spark.stop()