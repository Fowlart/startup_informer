import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,input_file_name
from pyspark_jobs.__init__ import get_labelled_message_schema, get_raw_schema_definition

if __name__ == "__main__":
    spark = (SparkSession.builder.getOrCreate())

    # get script dir path
    dir_path = os.path.dirname(os.path.realpath(__file__))

    labelled_data_path = os.path.join(dir_path, "../../../labelled_messages_for_training")

    all_ingested_messages_path = os.path.join(dir_path, "../../../dialogs/")

    labelled_messages_for_training = (spark
                                      .read
                                      .schema(get_labelled_message_schema())
                                      .json(labelled_data_path))

    labelled_messages_for_training.printSchema()

    all_messages_df =(spark
          .read
          .option("multiline", "true")
     .option("recursiveFileLookup", "true")
     .schema(get_raw_schema_definition())
     .json(all_ingested_messages_path)
     .select("dialog", "user.id", "message_date", "message_text")
     .withColumnRenamed("id", "user_id")
     .filter(col("user_id") == "553068238")
     .drop("user_id")
     .withColumn("file_name",input_file_name()))

    all_messages_df.show(truncate=False)



    labels_dict = {
        "projectFileVersion": "2022-05-01",
        "stringIndexType": "Utf16CodeUnit",
        "metadata": {
            "projectKind": "CustomSingleLabelClassification",
            "storageInputContainerName": "telegram-messages",
            "settings": {},
            "projectName": "tg-message-classification",
            "multilingual": "false",
            "description": "Project-description",
            "language": "uk"
        },
        "assets": {
            "projectKind": "CustomSingleLabelClassification",
            "classes": [
                {
                    "category": "food"
                },
                {
                    "category": "schedule"
                },
                {
                    "category": "toxic"
                }
            ],
            "documents": [
                {
                    "location": "{DOCUMENT-NAME}",
                    "language": "{LANGUAGE-CODE}",
                    "dataset": "{DATASET}",
                    "class": {
                        "category": "Class2"
                    }
                },
                {
                    "location": "{DOCUMENT-NAME}",
                    "language": "{LANGUAGE-CODE}",
                    "dataset": "{DATASET}",
                    "class": {
                        "category": "Class1"
                    }
                }
            ]
        }
    }

    spark.stop()
