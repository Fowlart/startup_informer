import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, lit, split, size
from pyspark.sql.types import StringType

from pyspark_jobs.__init__ import get_labelled_message_schema, get_raw_schema_definition
import json
from utilities.AzureLanguageServiceManagement import LanguageServiceManagement
from utilities.AzureStorageContainerBlobManagement import AzureStorageContainerBlobManagement

if __name__ == "__main__":

    os.environ['PYSPARK_PYTHON'] = sys.executable

    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = (SparkSession.builder.getOrCreate())

    # get script dir path
    dir_path = os.path.dirname(os.path.realpath(__file__))

    labelled_data_path = os.path.join(dir_path, "../../../labelled_messages_for_training")

    all_ingested_messages_path = os.path.join(dir_path, "../../../dialogs/")

    labelled_messages_for_training = (spark
                                      .read
                                      .schema(get_labelled_message_schema())
                                      .json(labelled_data_path))

    labelled_messages_for_training = (
        labelled_messages_for_training
        .select("message_text", "category")
        .filter(col("category") != "undefined")
    )

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
     .withColumn("file_name", input_file_name())
     .withColumn("file_name",split(col("file_name"),"/").getItem(size(split(col("file_name"),"/"))-1))
     .withColumn("category",lit(None).cast(StringType())))

    result_df = ((all_messages_df.alias("all")
                 .join(labelled_messages_for_training.alias("labeled"), on="message_text", how="inner")
                 .select(
                    col("message_date"),
                    col("message_text"),
                    col("file_name"),
                    col("labeled.category").alias("category")))
                 .distinct())


    training, test = result_df.randomSplit([0.7, 0.3], 45)

    combined_list = training.rdd.map(lambda row: {
        "location": row.file_name,
        "language": "uk",
        "dataset": "Train",
        "class": {
            "category": row.category
        }
    }).collect() + test.rdd.map(lambda row: {
        "location": row.file_name,
        "language": "uk",
        "dataset": "Test",
        "class": {
            "category": row.category
        }
    }).collect()

    project_name = "tg-message-classification"
    container_name = "telegram-messages"
    model_name = "first_model"
    lang_management = LanguageServiceManagement()
    container_management = AzureStorageContainerBlobManagement(container_name=container_name)

    labels_dict = {
        "projectFileVersion": "2022-05-01",
        "stringIndexType": "Utf16CodeUnit",
        "metadata": {
            "projectKind": "customSingleLabelClassification",
            "storageInputContainerName": "telegram-messages",
            "settings": {},
            "projectName": project_name,
            "multilingual": "false",
            "description": "Project-description",
            "language": "uk"
        },
        "assets": {
            "projectKind": "customSingleLabelClassification",
            "classes": [
                {
                    "category": "food"
                },
                {
                    "category": "schedule"
                },
                {
                    "category": "toxic"
                },
                {
                    "category": "children"
                }
            ],
            "documents": combined_list
        }
    }

    print("Label file: ")
    print(json.dumps(labels_dict, indent=4, ensure_ascii=False))


    files_to_export =result_df.rdd.map( lambda row: {
        "file_name": row.file_name,
        "message_date": row.message_date,
        "message_text": row.message_text,
        "category": row.category}).collect()

    print("Messages to upload:")
    for x in files_to_export:
        print(x)

    (lang_management
     .remove_language_service_single_label_classification_project(project_name=project_name))

    container_management.clear_container()

    for f in files_to_export:
        container_management.save_to_blob(f["file_name"],f["message_text"])

    (lang_management.create_language_service_single_label_classification_project(project_name=project_name, generated_label_file=labels_dict))

    lang_management.start_training(project_name=project_name, model_name=model_name)

    lang_management.deploy_model(model_name=model_name, project_name=project_name, deployment_name=model_name)

    spark.stop()
