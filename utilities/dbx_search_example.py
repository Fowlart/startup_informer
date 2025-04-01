from databricks.vector_search.client import VectorSearchClient
import pprint
import os
from dotenv import load_dotenv

if __name__=="__main__":

    load_dotenv("./../.env")

    vsc = VectorSearchClient(
        # dev-2
        workspace_url=os.getenv('workspace_url'),
        ## grand_central_general
        service_principal_client_id=os.getenv('service_principal_client_id'),

        service_principal_client_secret=os.getenv('service_principal_client_secret')
    )

    index = vsc.get_index(endpoint_name="art-vec-endpoint", index_name="grand_central_dev.digitalshelf.pdp_test_index")

    results: str = index.similarity_search(num_results=3, columns=["product_description"], query_text="your sanctuary")

    pprint.pprint(results, compact=True)