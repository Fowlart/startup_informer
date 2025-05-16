import json
import os
import requests
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import (TextAnalyticsClient, SingleLabelClassifyAction)


class LanguageServiceManagement(object):

    def __init__(self):
        pass

    def _track_operation(self, operation_location: str):
        import time

        headers = {
            "Ocp-Apim-Subscription-Key": os.getenv("AZURE_LANGUAGE_KEY")
        }

        while True:
            try:
                response = requests.get(operation_location, headers=headers)
                response.raise_for_status()
                try:
                    response_data = response.json()
                    status = response_data.get("status")

                    if status in ["succeeded", "failed"]:
                        print(f"Operation {status}.")
                        print(f"Response: {json.dumps(response_data, indent=4)}")

                        break
                except (ValueError, KeyError) as parse_error:
                    print(f"Failed to parse response: {parse_error}")
                    break

                time.sleep(2)

            except requests.exceptions.RequestException as e:
                print(f"Error while tracking operation: {e}")
                break

    def create_language_service_single_label_classification_project(self, project_name: str,
                                                                    generated_label_file: dict[str, str]):

        api_key = os.getenv("AZURE_LANGUAGE_KEY")
        endpoint = os.getenv("AZURE_LANGUAGE_ENDPOINT")

        headers = {
            "Ocp-Apim-Subscription-Key": f"{api_key}"
        }

        url = f"{endpoint}/language/authoring/analyze-text/projects/{project_name}/:import?api-version=2022-05-01"
        print(f"Sending request: {url}")
        print(headers)
        print(json.dumps(generated_label_file))

        try:
            response = requests.post(url, headers=headers, json=generated_label_file)
            response.raise_for_status()
            print(f"Status code: {response.status_code}")
            print(f"Track project deployment: {response.headers.get('operation-location')}")
            self._track_operation(response.headers.get("operation-location"))

        except requests.exceptions.RequestException as e:
            print(f": {e}")

    def remove_language_service_single_label_classification_project(self, project_name: str):
        api_key = os.getenv("AZURE_LANGUAGE_KEY")
        endpoint = os.getenv("AZURE_LANGUAGE_ENDPOINT")

        headers = {
            "Ocp-Apim-Subscription-Key": f"{api_key}"
        }

        url = f"{endpoint}/language/authoring/analyze-text/projects/{project_name}?api-version=2022-05-01"
        print(f"Sending delete request: {url}")
        print(headers)

        try:
            response = requests.delete(url, headers=headers)
            response.raise_for_status()
            print(f"Project {project_name} successfully removed. Status code: {response.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"Failed to delete project '{project_name}': {e}")

    def classify_documents_single_category(self,
                                           project_name: str,
                                           deployment_name: str,
                                           documents: list[str]):

        endpoint = os.getenv("AZURE_LANGUAGE_ENDPOINT")
        key = os.getenv("AZURE_LANGUAGE_KEY")

        deployment_name = deployment_name

        text_analytics_client = TextAnalyticsClient(
            endpoint=endpoint,
            credential=AzureKeyCredential(key),
        )

        poller = text_analytics_client.begin_analyze_actions(
            documents,
            actions=[
                SingleLabelClassifyAction(
                    project_name=project_name,
                    deployment_name=deployment_name
                ),
            ],
        )

        document_results = poller.result()
        for doc, classification_results in zip(documents, document_results):
            print(f"{doc} <-> {classification_results}")

    def start_training(self, model_name: str, project_name: str):

        endpoint = os.getenv("AZURE_LANGUAGE_ENDPOINT")
        key = os.getenv("AZURE_LANGUAGE_KEY")

        headers = {
            "Ocp-Apim-Subscription-Key": key
        }

        body = {
            "modelLabel": f"{model_name}",
            "trainingConfigVersion": "2022-05-01",
            "evaluationOptions": {
                "kind": "manual"
            }
        }

        url = f"{endpoint}/language/authoring/analyze-text/projects/{project_name}/:train?api-version=2022-05-01"
        print(f"Sending training request for project: {project_name}")
        print(f"Request URL: {url}")

        try:
            response = requests.post(url, headers=headers, json=body)
            response.raise_for_status()
            print(f"Training initiated successfully. Status code: {response.status_code}")

            operation_location = response.headers.get("operation-location")
            if operation_location:
                print(f"Track training operation at: {operation_location}")
                self._track_operation(operation_location)

            else:
                print("Operation location not provided in the response.")
        except requests.exceptions.RequestException as e:
            print(f"Failed to start training for project '{project_name}': {e}")


    def deploy_model(self, model_name: str, project_name: str, deployment_name: str):

        endpoint = os.getenv("AZURE_LANGUAGE_ENDPOINT")
        key = os.getenv("AZURE_LANGUAGE_KEY")
        headers = {
            "Ocp-Apim-Subscription-Key": key
        }

        body = {
            "trainedModelLabel": f"{model_name}"
        }

        url = f"{endpoint}/language/authoring/analyze-text/projects/{project_name}/deployments/{deployment_name}?api-version=2022-05-01"
        print(f"Sending deployment request for project: {project_name}")
        print(f"Request URL: {url}")

        try:
            response = requests.put(url, headers=headers, json=body)
            response.raise_for_status()
            print(f"Deployment initiated successfully. Status code: {response.status_code}")

            operation_location = response.headers.get("operation-location")

            if operation_location:
                print(f"Track training operation at: {operation_location}")
                self._track_operation(operation_location)

            else:
                print("Operation location not provided in the response.")
        except requests.exceptions.RequestException as e:
            print(f"Failed to start training for project '{project_name}': {e}")


if __name__ == "__main__":
    management_client = LanguageServiceManagement()

    documents = [
        "Ти поганий!",
        "Чому не відписуєш???????",
        "Ти невдячний!",
        "Не псуй мені нерви!",
        "Люблю тебе сильно!",
        "Купи грінки з хумосом",
        "Принеси додому 3 пляшки води",
        "Графік на понеділок: 9:00 - робота",
        "Де зараз Вдадьо?"]

    management_client.classify_documents_single_category(
        deployment_name="first_model",
        documents=documents,
        project_name="tg-message-classification"
    )
