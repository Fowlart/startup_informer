from pyspark.sql import SparkSession
from pyspark.ml.feature import SQLTransformer

from studies.spark_MLlib.transformers import PATH_TO_DATA_ROOT

if __name__=="__main__":

    spark = (SparkSession
               .builder
               .appName("basic-ml-pipline")
               .getOrCreate())

    sales = (spark
             .read
             .format("csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .load(f"{PATH_TO_DATA_ROOT}/retail-data/by-day/*.csv")
             .coalesce(5)
             .where("Description IS NOT NULL"))

    sales.printSchema()

    basicTransformation = SQLTransformer().setStatement(
        """SELECT CustomerID, 
            SUM(Quantity) AS quantity_sum, 
            count(1) AS rows_count 
            FROM __THIS__ 
            GROUP BY CustomerID""")

    basicTransformation.transform(sales).show()

    spark.stop()