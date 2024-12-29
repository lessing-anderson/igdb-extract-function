import azure.functions as func
import logging
import uuid
import os
import shutil
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from api_igdb_handler import ApiIGDBHandler

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="igdb/extract")
def igdb_extract_function(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    #Parameters
    req_body = req.get_json()
    endpoint = req_body.get('endpoint') 
    extract_type = req_body.get('extract_type')
    delta_days = req_body.get('delta_days', None)
    account_name = req_body.get('account_name')
    storage_path = req_body.get('storage_path')
    exec_uuid = uuid.uuid4()

    #Get Configs AKV
    credential = DefaultAzureCredential()

    key_vault_url = "https://akv-para-testes.vault.azure.net/"
    client = SecretClient(vault_url=key_vault_url, credential=credential)
    client_id = client.get_secret("valueClientIdTwitch").value
    client_secret = client.get_secret("secretTwitch").value

    #Exec Extraction
    logging.info('Criando classe de ingestão...')
    ingestor = ApiIGDBHandler(client_id, client_secret)  
    logging.info('\n############################################')
    logging.info(f'Executando para endpoint: {endpoint}')
    ingestor.process(endpoint, exec_uuid, extract_type, delta_days)

    #Save in container raw
    container_name = "raw"
    account_url = f"https://{account_name}.blob.core.windows.net"
    source_folder = f'/tmp/{endpoint}/{exec_uuid}'

    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    container_client = blob_service_client.get_container_client(container_name)

    for root, dirs, files in os.walk(source_folder):
            for file_name in files:
                local_file_path = os.path.join(root, file_name)
                
                relative_path = os.path.relpath(local_file_path, "/tmp")
                blob_path = storage_path + relative_path
                blob_path = blob_path.replace("\\", "/")
                
                blob_client = container_client.get_blob_client(blob_path)
                with open(local_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)

                logging.info(f"Arquivo '{local_file_path}' carregado como '{blob_path}'")
    
    #Delete temp files
    shutil.rmtree(f'/tmp/{endpoint}/{exec_uuid}', ignore_errors=True)
        
    return func.HttpResponse(
            "This HTTP triggered function executed successfully.",
            status_code=200
    )