from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

if __name__=="__main__":

    spark = (SparkSession
               .builder
               .appName("vector-assembler")
               .getOrCreate())

    fakeIntDF = (spark.read.parquet(f"./../../../synthetic_data/simple-ml-integers/"))

    va = (VectorAssembler()
          .setInputCols(["int1","int2","int3"])
          .setOutputCol("assembled"))

    va.transform(fakeIntDF).show()

    spark.stop()