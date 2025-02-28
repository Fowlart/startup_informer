from pyspark.ml.feature import Bucketizer
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as f

if __name__=="__main__":

    spark = (SparkSession
               .builder
               .appName("bucketizer")
               .getOrCreate())

    count_df = spark.range(20).select(f.col("id").cast(DoubleType()))

    bucketBorders = [-1.0, 5.0, 10.0, 250.0, 600.0]

    bucketer = Bucketizer().setSplits(bucketBorders).setInputCol("id")

    bucketer.transform(count_df).show()

    spark.stop()