from pyspark.sql import SparkSession
from pyspark.ml.feature import MinMaxScaler

if __name__=="__main__":

    spark = (SparkSession.builder.appName("min-max-scaler").getOrCreate())

    df_for_scale = spark.read.parquet(f"./../../../synthetic_data/simple-ml-scaling/")

    sScaler = (MinMaxScaler()
               .setInputCol("features")
               .setOutputCol("scaled_features")
               .setMin(5)
               .setMax(10))

    sScaler.fit(df_for_scale).transform(df_for_scale).show(truncate=False)

    spark.stop()