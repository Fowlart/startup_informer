from pyspark.ml import Pipeline
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import col, regexp_extract, length as spark_length, transform, filter as spark_filter
from sparknlp import DocumentAssembler
from sparknlp.annotator import LemmatizerModel, Tokenizer, DistilBertForSequenceClassification, Doc2VecApproach, \
    ClassifierDLApproach
import sparknlp
import os

def _filter_words_with_digits(x: col)->Column:

    return regexp_extract(x, r"\d", 0) == ""

def _get_internal_field(struct: Column) -> Column:

    return struct.getField("result")

if __name__ == "__main__":

    dir_path = os.path.dirname(os.path.realpath(__file__))

    spark = (sparknlp.start())

    df = (spark
          .read
          .json("/home/artur/PycharmProjects/startup_informer/labelled_messages_for_training"))


    df.orderBy(col("category")).show(truncate=False, n=50)

    documentAssembler = (
        DocumentAssembler()
        .setInputCol("message_text")
        .setOutputCol("document"))


    tokenizer = (
        Tokenizer()
        .setInputCols("document")
        .setOutputCol("tokens_0"))


    lemmatizer: LemmatizerModel = (
        LemmatizerModel
        .pretrained("lemma", "uk")
        .setInputCols(["tokens_0"])
        .setOutputCol("tokens_1"))

    embeddings = (Doc2VecApproach()
        .setInputCols(["tokens_1"])
        .setOutputCol("sentence_embeddings"))

    classsifier_dl_trained = (ClassifierDLApproach()
                              .setInputCols(["sentence_embeddings"])
                              .setOutputCol("class")
                              .setLabelColumn("category")
                              .setMaxEpochs(5)
                              .setLr(0.001)
                              .setBatchSize(16)
                              .setEnableOutputLogs(True))

    pipline = Pipeline(stages=[documentAssembler, tokenizer, lemmatizer, embeddings, classsifier_dl_trained])

    transformer = pipline.fit(df)

    # test
    test_phrase = [
                      (1,"Ти поганий!"),

                      (2,"Чому не відписуєш???????"),

                      (3,"Ти невдячний!"),

                      (4,"Не псуй мені нерви!"),

                      (5, "Люблю тебе сильно!"),

                      (6, "Чекай , бл")]

    test_phrase_df = spark.createDataFrame(test_phrase,["id","message_text"])

    test_phrase_df.show(truncate=False)

    resul  = transformer.transform(test_phrase_df)

    resul.select("message_text","class").show(truncate=False)

    spark.stop()