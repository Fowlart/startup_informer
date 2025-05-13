from azure.identity import DefaultAzureCredential
from azure.mgmt.cognitiveservices import CognitiveServicesManagementClient
import os

from azure.mgmt.cognitiveservices.models import Usage


class CognitiveServiceManagement(object):

    def __init__(self):
        pass


    def get_CognitiveServicesManagementClient(self):
        sub_id = os.getenv("AZURE_MAIN_SUBSCRIPTION_ID")
        return CognitiveServicesManagementClient(credential=DefaultAzureCredential(), subscription_id=sub_id)


if __name__=="__main__":

    c = CognitiveServiceManagement()
    client = c.get_CognitiveServicesManagementClient()


    for use in client.usages.list(location="East US 2"):
        casted_use: Usage =use
        print(casted_use)
