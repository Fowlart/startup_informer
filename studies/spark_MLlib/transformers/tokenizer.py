from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer

if __name__=="__main__":

    spark = (SparkSession
               .builder
               .appName("tokenizer")
               .getOrCreate())

    sales = (spark
             .read
             .format("csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .load(f"./../../../synthetic_data/retail-data/by-day/*.csv")
             .coalesce(5)
             .where("Description IS NOT NULL"))

    sales.printSchema()

    tkn = Tokenizer().setInputCol("Description").setOutputCol("tokenized_description")

    tkn.transform(sales).show(truncate=False)