from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

from studies.spark_MLlib.transformers import PATH_TO_DATA_ROOT

if __name__=="__main__":

    spark = (SparkSession
               .builder
               .appName("basic-ml-pipline")
               .getOrCreate())

    fakeIntDF = (spark.
                 read.
                 parquet(f"{PATH_TO_DATA_ROOT}/simple-ml-integers"))

    va = VectorAssembler().setInputCols(["int1","int2","int3"]).setOutputCol("assembled")

    va.transform(fakeIntDF).show()