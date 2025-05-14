from azure.identity import DefaultAzureCredential
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
import os

import requests

from azure.mgmt.cognitiveservices.models import Usage


class CognitiveServiceManagement(object):

    def __init__(self):
        pass


    def get_CognitiveServicesManagementClient(self):
        sub_id = os.getenv("AZURE_MAIN_SUBSCRIPTION_ID")
        return CognitiveServicesManagementClient(credential=DefaultAzureCredential(), subscription_id=sub_id)


    def create_language_service_single_label_classification_project(self, project_name: str,  generated_label_file: dict[str,str]):

        api_key = os.getenv("AZURE_LANGUAGE_KEY")
        endpoint = os.getenv("AZURE_LANGUAGE_ENDPOINT")

        headers = {
            "Ocp-Apim-Subscription-Key": f"{api_key}"
        }

        url = f"{endpoint}/language/authoring/analyze-text/projects/{project_name}/:import?api-version=2022-05-01"

        print(f"Sending request: {url}")

        print(headers)


        try:
            response = requests.post(url, headers=headers, json=generated_label_file)
            response.raise_for_status()
            print(f"Status code: {response.status_code}")
            print(f"Track project deployment: {response.headers.get('operation-location')}")

        except requests.exceptions.RequestException as e:
            print(f": {e}")


if __name__=="__main__":

    c = CognitiveServiceManagement()
    client = c.get_CognitiveServicesManagementClient()

    for use in client.usages.list(location="East US 2"):
        casted_use: Usage =use
        print(casted_use)
