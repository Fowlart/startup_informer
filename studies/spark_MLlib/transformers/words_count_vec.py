from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import RegexTokenizer, CountVectorizer, StopWordsRemover

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
           # .setGaps(False)
           .setToLowercase(True))

    stop_words = StopWordsRemover().loadDefaultStopWords("english")

    stop_words.extend(["t-light","7","6"])

    words_remover = (StopWordsRemover()
                     .setInputCol("description_output")
                     .setOutputCol("description_output_without_stop_words")
                     .setStopWords(stop_words)
                     )

    result = words_remover.transform(tkn.transform(df))

    count_vectorizer = (CountVectorizer()
    .setInputCol("description_output_without_stop_words")
    .setOutputCol("count_vector")
    .setVocabSize(100)
    .setMinTF(1)
    .setMaxDF(2))

    (count_vectorizer
     .fit(result)
     .transform(result)
     .show(truncate=False))

    spark.stop()