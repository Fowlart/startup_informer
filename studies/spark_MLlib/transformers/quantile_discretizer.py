from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as f

if __name__=="__main__":

    spark = (SparkSession
               .builder
               .appName("quantile_discretizer")
               .getOrCreate())

    count_df = spark.range(20).select(f.col("id").cast(DoubleType()))

    bucketer = QuantileDiscretizer().setNumBuckets(5).setInputCol("id")

    fittedBucketer = bucketer.fit(count_df)

    fittedBucketer.transform(count_df).show(truncate=False)

    spark.stop()