from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer

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

    tkn.transform(df).show(truncate=False)

    spark.stop()