from pyspark.sql import SparkSession
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer, NGram

if __name__=="__main__":

    spark = (SparkSession.builder.getOrCreate())

    df = (spark
          .read
          .option("header","true")
          .csv(f"./../../../synthetic_data/retail-data/all/online-retail-dataset.csv"))

    tkn = (RegexTokenizer()
           .setInputCol("Description")
           .setOutputCol("description_output")
           .setPattern(" ")
           #.setGaps(False)
           .setToLowercase(True))

    stop_words = StopWordsRemover().loadDefaultStopWords("english")

    stop_words.extend(["white",
                       "red",
                       "vintage",
                       "jigsaw",
                       "block",
                       "playhouse",
                       "ornament",
                       "kitchen",
                       "bedroom",
                       "mug",
                       "babushka",
                       "princess",
                       "building",
                       "word",
                       "box",
                       "warmer",
                       "union",
                       "t-light",
                       "hearts",
                       "flag",
                       "7",
                       "6"])

    wrmvr = (StopWordsRemover()
             .setInputCol("description_output")
             .setOutputCol("description_output_without_stop_words")
             .setStopWords(stop_words)
             )

    result = wrmvr.transform(tkn.transform(df))

    (NGram()
     .setInputCol("description_output_without_stop_words")
     .setN(2)
     .setOutputCol("2_n_gram")
     .transform(result.select("description_output_without_stop_words"))
     .show(truncate=False))

    (NGram()
     .setInputCol("description_output_without_stop_words")
     .setN(3)
     .setOutputCol("3_n_gram")
     .transform(result.select("description_output_without_stop_words"))
     .show(truncate=False))


    spark.stop()