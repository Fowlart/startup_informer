from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoder, StringIndexer

if __name__=="__main__":

    spark = (SparkSession.builder.appName("one-hot-encoder").getOrCreate())

    df = (spark.read.json(f"./../../../synthetic_data/simple-ml"))

    string_label_indexer = (StringIndexer().setInputCol("color").setOutputCol("color_index"))

    color_labelled_df = string_label_indexer.fit(df).transform(df.select("color"))

    color_labelled_df.show(truncate=False)

    ohe = OneHotEncoder().setInputCol("color_index")

    ohe.fit(color_labelled_df).transform(color_labelled_df).show(truncate=False)

    spark.stop()