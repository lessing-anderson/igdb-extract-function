import azure.functions as func
import logging
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from api_igdb_handler import ApiIGDBHandler

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="igdb/extract")
def igdb_extract_function(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    #Parameters
    key_vault_url = "https://akv-para-testes.vault.azure.net/"
        
    req_body = req.get_json()
    endpoint = req_body.get('endpoint') 
    extract_type = req_body.get('extract_type')
    delta_days = req_body.get('delta_days', None)
    storage_path = req_body.get('storage_path')

    #Configs
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=key_vault_url, credential=credential)

    client_id = client.get_secret("valueClientIdTwitch").value
    client_secret = client.get_secret("secretTwitch").value

    logging.info('Criando classe de ingestão...')
    ingestor = ApiIGDBHandler(client_id, client_secret, storage_path)  

    logging.info('\n############################################')
    logging.info(f'Executando para endpoint: {endpoint}')
    ingestor.process(endpoint, extract_type, delta_days)
        
    return func.HttpResponse(
            "This HTTP triggered function executed successfully.",
            status_code=200
    )