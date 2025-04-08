from pyspark.ml import Pipeline
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import col, regexp_extract, length as spark_length, transform, filter as spark_filter, array
from sparknlp import DocumentAssembler
from sparknlp.annotator import LemmatizerModel, Tokenizer, Doc2VecApproach, \
    ClassifierDLApproach, BertSentenceEmbeddings
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
          .json("/home/artur/PycharmProjects/startup_informer/labelled_messages_for_training")
          .withColumn("categories", array(col("category")))
          )


    df.orderBy(col("category")).show(truncate=False, n=50)

    documentAssembler = (
        DocumentAssembler()
        .setInputCol("message_text")
        .setOutputCol("document")
        .setCleanupMode("shrink")
    )


    tokenizer = (
        Tokenizer()
        .setInputCols("document")
        .setOutputCol("tokens_0"))


    lemmatizer: LemmatizerModel = (
        LemmatizerModel
        .pretrained("lemma", "uk")
        .setInputCols(["tokens_0"])
        .setOutputCol("tokens_1"))

    # todo: try different embeddings model
    embeddings = (Doc2VecApproach()
        .setInputCols(["tokens_1"])
        .setOutputCol("sentence_embeddings"))

    bert_sent_embeddings = (BertSentenceEmbeddings.pretrained('sent_small_bert_L8_512')
        .setInputCols(["document"])
        .setOutputCol("sentence_embeddings"))

    classsifier_dl_trained = (ClassifierDLApproach()
                              .setInputCols(["sentence_embeddings"])
                              .setOutputCol("class")
                              .setLabelColumn("category")
                              .setBatchSize(128)
                              .setMaxEpochs(5)
                              .setLr(1e-3)
                              .setEnableOutputLogs(True)
                              .setValidationSplit(0.1))

    pipline = Pipeline(stages=[documentAssembler, tokenizer, lemmatizer, bert_sent_embeddings, classsifier_dl_trained])

    transformer = pipline.fit(df)

    # test
    test_phrase = [
                      (1,"Ти поганий!"),

                      (2,"Чому не відписуєш???????"),

                      (3,"Ти невдячний!"),

                      (4,"Не псуй мені нерви!"),

                      (5, "Люблю тебе сильно!"),

                      (6, "Чекай , бл"),

                      (7, "Купи грінки з хумосом")

    ]

    test_phrase_df = spark.createDataFrame(test_phrase,["id","message_text"])

    resul  = transformer.transform(test_phrase_df)

    resul.select("message_text","class").show(truncate=False)

    spark.stop()