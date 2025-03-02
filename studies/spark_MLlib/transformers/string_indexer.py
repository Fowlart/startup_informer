from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, IndexToString

if __name__=="__main__":

    spark = (SparkSession.builder.appName("string_indexer").getOrCreate())

    df = (spark.read.json(f"./../../../synthetic_data/simple-ml"))

    lbl_indexer = (
        StringIndexer()
        .setInputCol("lab")
        .setOutputCol("labelled_ind")
        .setHandleInvalid("skip")
    )

    indexed_df = lbl_indexer.fit(df).transform(df)

    indexed_df.show(truncate=False)

    (IndexToString()
     .setInputCol("labelled_ind")
     .setOutputCol("lab_from_index")
     .transform(indexed_df).show())

    spark.stop()