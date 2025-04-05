import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline

if __name__ == "__main__":

    spark = sparknlp.start()

    document = DocumentAssembler().setInputCol("text").setOutputCol("document")

    token = Tokenizer().setInputCols(["document"]).setOutputCol("token")

    normalizer = Normalizer().setInputCols(["token"]).setOutputCol("normal")

    vive_kn = (ViveknSentimentApproach()
        .setInputCols(["document", "normal"])
        .setSentimentCol("train_sentiment")
        .setOutputCol("result_sentiment"))

    finisher = (
        Finisher()
        .setInputCols(["result_sentiment"])
        .setOutputCols("final_sentiment"))

    pipeline = Pipeline().setStages([document, token, normalizer, vive_kn, finisher])

    training = spark.createDataFrame([
        ("I really liked this movie!", "positive"),
        ("The cast was horrible", "negative"),
        ("Never going to watch this again or recommend it to anyone", "negative"),
        ("It's a waste of time", "negative"),
        ("I loved the protagonist", "positive"),
        ("The music was really really good", "positive")
    ]).toDF("text", "train_sentiment")

    pipelineModel = pipeline.fit(training)

    data = spark.createDataFrame([
        ["I recommend this movie"],
        ["Dont waste your time!!!"]
    ]).toDF("text")

    result = pipelineModel.transform(data)

    result.select("final_sentiment").show(truncate=False)