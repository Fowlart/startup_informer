from pyspark.sql import SparkSession
from pyspark.ml.feature import MaxAbsScaler

if __name__=="__main__":

    spark = (SparkSession.builder.appName("max-abs-scaler").getOrCreate())

    df_for_scale = spark.read.parquet(f"./../../../synthetic_data/simple-ml-scaling/")

    sScaler = (MaxAbsScaler().setInputCol("features").setOutputCol("scaled_features"))

    sScaler.fit(df_for_scale).transform(df_for_scale).show(truncate=False)

    spark.stop()