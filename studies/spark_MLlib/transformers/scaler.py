from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler

if __name__=="__main__":

    spark = (SparkSession
               .builder
               .appName("scaler")
               .getOrCreate())

    scaleDF = (spark
               .read
               .parquet("./../../synthetic_data/simple-ml-scaling/"))

    scaleDF.printSchema()

    ss = (StandardScaler()
          .setInputCol("features")
          .setOutputCol("scaled_features"))

    (ss
      # It will pass over dataframe to check values before normalizing
     .fit(scaleDF)
     .transform(scaleDF)
     .show(truncate=False))

    spark.stop()