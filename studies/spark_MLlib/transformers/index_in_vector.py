from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.linalg import Vectors

if __name__=="__main__":

    spark = (SparkSession
             .builder
             .appName("find_categorical_data_in_a_vector")
             .getOrCreate())

    idxIn = spark.createDataFrame([
        (Vectors.dense(1, 2, 3), 1),
        (Vectors.dense(2, 5, 6), 2),
        (Vectors.dense(1, 8, 9), 3)
    ]).toDF("features","label")

    idxr = (VectorIndexer()
            .setInputCol("features")
            .setOutputCol("idxed_features")
            .setMaxCategories(2))

    idxr.fit(idxIn).transform(idxIn).show()

    spark.stop()