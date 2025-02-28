from pyspark.sql import SparkSession
from pyspark.ml.feature import SQLTransformer

if __name__=="__main__":

    spark = (SparkSession
               .builder
               .appName("sql-transformers")
               .getOrCreate())

    sales = (spark
             .read
             .format("csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .load(f"./../../synthetic_data/retail-data/by-day/*.csv")
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