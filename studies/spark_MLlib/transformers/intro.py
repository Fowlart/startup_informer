from pyspark.sql import SparkSession
from pyspark.ml.feature import RFormula

PATH_TO_DATA_ROOT = "./synthetic_data"

if __name__=="__main__":

    spark = (SparkSession
               .builder
               .appName("basic-ml-pipline")
               .getOrCreate())

    df = (spark
          .read
          .json(f"{PATH_TO_DATA_ROOT}/simple-ml"))

    df.show(300, truncate=False)

    #todo: Suppose that we want to train a classification model where we hope
    # to predict a binary variable—the label—from the other values

    # in this case we want to use all available variables
    supervised = RFormula(formula="lab ~ . + color:value1 + color:value2")

    fittedRF = supervised.fit(df)

    preparedDF = fittedRF.transform(df)

    preparedDF.show()

    preparedDF.printSchema()

    spark.stop()