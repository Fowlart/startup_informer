from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import col, regexp_extract, length as spark_length, transform, filter as spark_filter
from sparknlp import DocumentAssembler
from sparknlp.annotator import LemmatizerModel, Tokenizer, DistilBertForSequenceClassification, Doc2VecApproach, \
    ClassifierDLApproach
from pyspark.ml.feature import Word2Vec
from pyspark_jobs.__init__ import get_schema_definition
import sparknlp

def _filter_words_with_digits(x: col)->Column:

    return regexp_extract(x, r"\d", 0) == ""

def _words_length_filter(x: col) -> Column:

    return spark_length(x)>=configuration["min_token_length"]

def _get_internal_field(struct: Column) -> Column:

    return struct.getField("result")

if __name__ == "__main__":

    spark = (sparknlp.start())

    df = (spark
          .read
          .option("multiline", "true")
          .json("../../../labelled_messages_for_training/"))

    documentAssembler = DocumentAssembler().setInputCol("message_text").setOutputCol("document")

    df = documentAssembler.transform(df)

    tokenizer = (
        Tokenizer()
        .setInputCols("document")
        .setOutputCol("tokens_0"))

    df = tokenizer.fit(df).transform(df)

    lemmatizer: LemmatizerModel = (
        LemmatizerModel
        .pretrained("lemma", "uk")
        .setInputCols(["tokens_0"])
        .setOutputCol("tokens_1"))

    df = lemmatizer.transform(df)

    embeddings = (Doc2VecApproach()
        .setInputCols(["tokens_1"])
        .setOutputCol("sentence_embeddings"))

    df = embeddings.fit(df).transform(df)

    classsifier_dl_trained = (ClassifierDLApproach()
                              .setInputCols(["sentence_embeddings"])
                              .setOutputCol("class")
                              .setLabelColumn("category")
                              .setMaxEpochs(5)
                              .setLr(0.001)
                              .setBatchSize(16)
                              .setEnableOutputLogs(True))

    trained = classsifier_dl_trained.fit(df)



    df = trained.transform(df)

    df.select("class").show(100)

    spark.stop()