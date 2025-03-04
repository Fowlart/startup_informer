from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoder, StringIndexer, Tokenizer

if __name__=="__main__":

    spark = (SparkSession.builder.getOrCreate())

    df = (spark.read.option("header","true").csv(f"./../../../synthetic_data/retail-data/all/online-retail-dataset.csv"))

    # df.show(truncate=False)

    tkn = Tokenizer().setInputCol("Description").setOutputCol("description_output")

    tkn.transform(df).show(truncate=False)

    spark.stop()