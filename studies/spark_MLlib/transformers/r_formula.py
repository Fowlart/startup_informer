from pyspark.sql import SparkSession
from pyspark.ml.feature import RFormula


if __name__=="__main__":

    spark = (SparkSession
               .builder
               .appName("r_formula")
               .getOrCreate())

    df = (spark
          .read
          .json(f"./../../synthetic_data/simple-ml"))

    # todo: Suppose that we want to train a classification model where we hope
    # to predict a binary variable—the label—from the other values
    # in this case we want to use all available variables
    supervised = RFormula(formula="lab ~ . + color:value1 + color:value2")

    fittedRF = supervised.fit(df)

    preparedDF = fittedRF.transform(df)

    preparedDF.show(300, truncate=False)

    preparedDF.printSchema()

    spark.stop()