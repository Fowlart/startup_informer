from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoder, StringIndexer

if __name__=="__main__":

    spark = (SparkSession.builder.appName("one-hot-encoder").getOrCreate())

    df = (spark.read.json(f"./../../../synthetic_data/simple-ml"))

    labeled = ( StringIndexer().setInputCol("color").setOutputCol("color_index"))

    colorLab = labeled.fit(df).transform(df.select("color"))

    # colorLab.show(truncate=False)

    ohe = OneHotEncoder().setInputCol("color_index")

    ohe.fit(colorLab).transform(colorLab).show(truncate=False)

    spark.stop()