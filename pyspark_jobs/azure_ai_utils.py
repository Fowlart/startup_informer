# todo: The problem with this module: if we use a function within a separate module outside the PySpark job script,
#  it is not visible by Spark in runtime. Can be resolved by packaging all the code inside a wheel
# utilities to work with Azure AI search services
from azure.core.credentials import AzureKeyCredential
# todo: add async here
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import AnalyzeTextOptions
from subprocess import Popen, PIPE
from azure.ai.textanalytics import TextAnalyticsClient

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


def _get_folder_with_scripts() -> str:
    return "azure_ai_scripts"

def _get_test_index_name()->str:
    return "fowlart-personal-index"

def _get_ai_search_endpoint() -> str:
    return f"https://fowlart-ai-search.search.windows.net"

def _get_language_service_endpoint() -> str:
    return "https://fowlart-language-service.cognitiveservices.azure.com"


def _get_terminal_command(file_name_to_execute: str) -> list[str]:
    linux_cmd = ['sh', f'../{_get_folder_with_scripts()}/{file_name_to_execute}.sh']
    return linux_cmd

def _get_azure_service_key(script_name: str) ->str:

    result = ""
    cmd = _get_terminal_command(script_name)
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    while True:
        line = proc.stdout.readline()
        if line != b'':
            the_line = line.decode("utf-8").strip()
            if "key is>" in the_line:
                result = the_line.split(">")[-1]
        else:
            break
    return result

def _get_search_service_key()->str:
    file_name = "ai_service_api_key"
    print(f"{bcolors.OKBLUE} Starting search service key extraction {bcolors.ENDC}")
    return _get_azure_service_key(file_name)

def _get_language_service_key()->str:
    file_name = "language_service_api_key"
    print(f"{bcolors.OKBLUE} Starting language service key extraction {bcolors.ENDC}")
    return _get_azure_service_key(file_name)

def _get_search_index_client() -> SearchIndexClient:
    service_endpoint = _get_ai_search_endpoint()
    key = _get_search_service_key()
    return SearchIndexClient(service_endpoint, AzureKeyCredential(key))

def _get_search_client(index_name: str = None) -> SearchClient:
    service_endpoint = _get_ai_search_endpoint()
    key = _get_search_service_key()
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


if __name__ == "__main__":

    content_en = """
    The quick brown fox jumps over the lazy dog
    """

    content_ukr ="""
    Наступну суботу-неділю 8-9 березня планую бути у Львові. Зможеш бути трохи вільнішим від своєї роботи у ці дні?
    """

    result= get_tokens(content_en,
               "en.microsoft",
               "fowlart-personal-index",
                       _get_search_index_client())

    print(result)

    tokens = _analyze_text(text=content_ukr, analyzer_name="uk.microsoft")

    print(type(tokens))

    for token in tokens:
        print(token)
        print(type(token))

    print("Show extracted keyphrases example:")
    print(extract_key_phrases(text=content_ukr, language="uk"))