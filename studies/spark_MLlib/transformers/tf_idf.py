from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array_contains
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF

if __name__=="__main__":
    spark = (SparkSession.builder.getOrCreate())

    df = (spark
          .read
          .option("header", "true")
          .csv(f"./../../../synthetic_data/retail-data/all/online-retail-dataset.csv")
          .filter(col("Description").isNotNull())
          )

    tkn = (RegexTokenizer()
           .setInputCol("Description")
           .setOutputCol("description_output")
           .setPattern(" ")
           .setToLowercase(True))

    stop_words = StopWordsRemover().loadDefaultStopWords("english")

    stop_words.extend(["t-light","7","6"])

    words_remover = (StopWordsRemover()
                     .setInputCol("description_output")
                     .setOutputCol("description_output_without_stop_words")
                     .setStopWords(stop_words))

    ready_for_tf_idf = (words_remover.transform(tkn.transform(df))
                        .filter(array_contains(col("description_output_without_stop_words"),"red")))

    tf = (
        HashingTF()
          .setInputCol("description_output_without_stop_words")
          .setOutputCol("tf_out")
          .setNumFeatures(10000))

    idf = (
        IDF()
        .setInputCol("tf_out")
        .setOutputCol("idf_out")
        .setMinDocFreq(2))

    tf_transformed = tf.transform(ready_for_tf_idf)

    (idf.fit(tf_transformed).transform(tf_transformed).select("description_output_without_stop_words","tf_out","idf_out")
     .show(truncate=False))

    spark.stop()