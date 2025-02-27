from pyspark.sql import SparkSession
from pyspark.ml.feature import RFormula, Tokenizer, StandardScaler

if __name__=="__main__":

    spark = (SparkSession
               .builder
               .appName("basic-ml-pipline")
               .getOrCreate())

    sales = (spark
             .read
             .format("csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .load("/home/artur/PycharmProjects/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")
             .coalesce(5)
             .where("Description IS NOT NULL"))

    fakeIntDF = (spark.
                 read.
                 parquet("/home/artur/PycharmProjects/Spark-The-Definitive-Guide/data/simple-ml-integers"))

    simpleDF = (spark.
                read.
                json("/home/artur/PycharmProjects/Spark-The-Definitive-Guide/data/simple-ml"))

    scaleDF = (spark
               .read
               .parquet("/home/artur/PycharmProjects/Spark-The-Definitive-Guide/data/simple-ml-scaling"))

    sales.printSchema()
    fakeIntDF.printSchema()
    simpleDF.printSchema()
    scaleDF.printSchema()

    # 1 - Tokenizer example
    tkn = Tokenizer().setInputCol("Description").setOutputCol("tokenized_description")
    # tkn.transform(sales).show()

    # 2 - Standard scaler example
    ss = (StandardScaler()
          .setInputCol("features")
          .setOutputCol("scaled_features"))

    (ss
      # It will pass over dataframe to check values before normalizing
     .fit(scaleDF)
     .transform(scaleDF)
     .show())



    spark.stop()