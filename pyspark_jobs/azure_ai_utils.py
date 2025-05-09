# todo: The problem with this module: if we use a function within a separate module outside the PySpark job script,
#  it is not visible by Spark in runtime. Can be resolved by packaging all the code inside a wheel
# utilities to work with Azure AI search services
# todo: add async here
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import AnalyzeTextOptions
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import (TextAnalyticsClient, SingleLabelClassifyAction)
import os

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def _get_test_index_name()->str:
    return "fowlart-personal-index"

def _get_ai_search_endpoint() -> str:
    return str(os.getenv("AI_SEARCH_ENDPOINT"))

def _get_language_service_endpoint() -> str:
    return str(os.getenv("AZURE_LANGUAGE_ENDPOINT"))

def _get_azure_ai_search_key() ->str:
    return str(os.getenv("AZURE_AI_SEARCH_KEY"))

def _get_language_service_key()->str:
    return str(os.getenv("AZURE_LANGUAGE_KEY"))

def _get_search_index_client() -> SearchIndexClient:
    service_endpoint = _get_ai_search_endpoint()
    key = _get_azure_ai_search_key()
    return SearchIndexClient(service_endpoint, AzureKeyCredential(key))

def _get_search_client(index_name: str = None) -> SearchClient:
    service_endpoint = _get_ai_search_endpoint()
    key = _get_azure_ai_search_key()
    index_name = _get_test_index_name() if (index_name is None) else index_name

    return SearchClient(endpoint=service_endpoint,
                        credential=AzureKeyCredential(key),
                        index_name=index_name)

def _analyze_text(text:str, analyzer_name: str, index_name: str=_get_test_index_name()):
    client: SearchIndexClient = _get_search_index_client()
    print(f"{bcolors.OKGREEN} text: {text} \n {bcolors.OKCYAN}analyzer: {analyzer_name} {bcolors.ENDC}")
    op: AnalyzeTextOptions = AnalyzeTextOptions(text=text,analyzer_name=analyzer_name)
    resp: dict[str] = client.analyze_text(index_name, op).as_dict()
    return resp.get("tokens")

def _get_text_analytics_client():
    ta_credential = AzureKeyCredential(_get_language_service_key())
    text_analytics_client = TextAnalyticsClient(
        endpoint=_get_language_service_endpoint(),
        credential=ta_credential)
    return text_analytics_client

def get_tokens(text:str, analyzer_name: str, index_name: str, client: SearchIndexClient) -> list[str]:
    op: AnalyzeTextOptions = AnalyzeTextOptions(text=text,analyzer_name=analyzer_name)
    resp: dict[str] = client.analyze_text(index_name, op).as_dict()
    return [str(el["token"]) for el in resp.get("tokens")]

def extract_key_phrases(text: str,
                        client: TextAnalyticsClient = _get_text_analytics_client(),
                        language: str = None
                        ) -> list[str]:
    result = []
    try:
        documents = [text]
        response = client.extract_key_phrases(documents=documents,language = language)[0]
        if not response.is_error:
            result = response.key_phrases
        else:
            print(response.id, response.error)

    except Exception as err:
        print("Encountered exception. {}".format(err))

    return result

def sample_classify_document_single_category(single_category_classify_project_name: str,
                                             single_category_classify_deployment_name: str,
                                             documents: list[str]):

    endpoint = _get_language_service_endpoint()
    key = _get_language_service_key()
    project_name = single_category_classify_project_name
    deployment_name = single_category_classify_deployment_name


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


if __name__ == "__main__":

    print("Showcase custom text classification: ")
    sample_classify_document_single_category(single_category_classify_project_name="tg-message-classification",
                                             single_category_classify_deployment_name="deployed_first_model",
                                             documents=[
                                                 "Ти поганий!",
                                                 "Чому не відписуєш???????",
                                                 "Ти невдячний!",
                                                 "Не псуй мені нерви!",
                                                 "Люблю тебе сильно!",
                                                 "Купи грінки з хумосом",
                                                 "Принеси додому 3 пляшки води",
                                                 "Графік на понеділок: 9:00 - робота",
                                                 "Де зараз Вдадьо?"])
