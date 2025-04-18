from pyspark.ml import Pipeline
from sparknlp import DocumentAssembler
from sparknlp.annotator import Tokenizer, ClassifierDLApproach, UniversalSentenceEncoder
import sparknlp
import os
import sys

if __name__ == "__main__":

    os.environ['PYSPARK_PYTHON'] = sys.executable

    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    dir_path = os.path.dirname(os.path.realpath(__file__))

    spark = sparknlp.start()

    df = (spark.read.json(f"{dir_path}/../../../labelled_messages_for_training"))

    documentAssembler = (
        DocumentAssembler()
        .setInputCol("message_text")
        .setOutputCol("document")
    )

    tokenizer = Tokenizer().setInputCols("document").setOutputCol("token")

    embeddings = (
        UniversalSentenceEncoder.pretrained()
        .setInputCols("document")
        .setOutputCol("sentence_embeddings")
    )

    classsifier_dl_trained = (ClassifierDLApproach()
                              .setInputCols(["sentence_embeddings"])
                              .setOutputCol("class")
                              .setLabelColumn("category")
                              .setBatchSize(128)
                              .setMaxEpochs(5)
                              .setLr(1e-3)
                              .setEnableOutputLogs(True)
                              .setValidationSplit(0.1))

    pipline = Pipeline(stages=[
        documentAssembler,
        tokenizer,
        embeddings,
        classsifier_dl_trained
    ])

    transformer = pipline.fit(df)

    # test
    test_phrase = [(1, "Ти поганий!"),
                   (2, "Чому не відписуєш???????"),
                   (3, "Ти невдячний!"),
                   (4, "Не псуй мені нерви!"),
                   (5, "Люблю тебе сильно!"),
                   (6, "Чекай , бл"),
                   (7, "Купи грінки з хумосом"),
                   (8, "Графік на понеділок: 9:00 - робота")]

    test_phrase_df = spark.createDataFrame(test_phrase, ["id", "message_text"])

    result = transformer.transform(test_phrase_df)


    (result.select("message_text", "class").show(truncate=False))

    spark.stop()
