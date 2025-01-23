import azure.functions as func
import azure.durable_functions as df
from cryptography.fernet import Fernet
import base64
import logging
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from api_igdb_handler import ApiIGDBHandler
from azure.storage.blob import BlobServiceClient

myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

cipher = Fernet(Fernet.generate_key())

credential = DefaultAzureCredential()

#Client function
@myApp.route(route="igdb/extract")
@myApp.durable_client_input(client_name="client")
async def igdb_extract_function(req: func.HttpRequest, client):

    #Parameters
    req_body = req.get_json()

    #Get secrets from AKV
    key_vault_url = "https://akv-para-testes.vault.azure.net/"
    client_akv = SecretClient(vault_url=key_vault_url, credential=credential)
    client_id = client_akv.get_secret("valueClientIdTwitch").value
    client_secret = client_akv.get_secret("secretTwitch").value

    #Encript secrets
    client_id_encripted = cipher.encrypt(client_id.encode())
    client_id_encripted_b64 = base64.b64encode(client_id_encripted).decode('utf-8')
    client_secret_encripted = cipher.encrypt(client_secret.encode())
    client_secret_encripted_b64 = base64.b64encode(client_secret_encripted).decode('utf-8')
    req_body.update({"client_id_encripted": client_id_encripted_b64, "client_secret_encripted": client_secret_encripted_b64})

    #Start the Durable Orchestration
    instance_id = await client.start_new('orchestrator', client_input=req_body)
    response = client.create_check_status_response(req, instance_id)

    return response

# Orchestrator
@myApp.orchestration_trigger(context_name="context")
def orchestrator(context):

    #Parameters
    parameters = context.get_input()
    exec_uuid = context.new_uuid()
    offset = 0
    has_more_data = True
    count_of_files = 0

    #Exec Loop Extraction/Load
    parameters.update({"uuid" : exec_uuid})

    while True:
        parameters.update({"offset": offset})

        extract_response = yield context.call_activity("extract", parameters)
        has_more_data = extract_response.get('has_more_data')
        file_name = extract_response.get('file_name')

        parameters.update({"file_name": file_name})
        yield context.call_activity("load", parameters)

        count_of_files += 1

        if has_more_data:
            offset += 500
        else:
            break

    return [exec_uuid, count_of_files]

# Activity
@myApp.activity_trigger(input_name="input")
def extract(input):

    #Parameters
    endpoint = input.get('endpoint') 
    extract_type = input.get('extract_type')
    delta_days = input.get('delta_days', None)
    client_id_encripted_b64 = input.get('client_id_encripted')
    client_secret_encripted_b64 = input.get('client_secret_encripted')
    exec_uuid = input.get('uuid')
    offset = input.get('offset')
    file_name = f"{endpoint}_{exec_uuid}_{offset}"

    #Create ingestor
    client_id_encripted = base64.b64decode(client_id_encripted_b64)
    client_id = cipher.decrypt(client_id_encripted).decode('utf-8')
    client_secret_encripted = base64.b64decode(client_secret_encripted_b64)
    client_secret = cipher.decrypt(client_secret_encripted).decode('utf-8')
    ingestor = ApiIGDBHandler(client_id, client_secret)     
    
    #Process
    has_more_data = ingestor.process(endpoint, exec_uuid, extract_type, delta_days, offset, file_name)

    extractResponse = {'has_more_data': has_more_data, 'file_name' : file_name}
    return extractResponse

# Activity
@myApp.activity_trigger(input_name="input")
def load(input):

    #Parameters
    account_name = input.get('account_name')
    storage_path = input.get('storage_path')
    file_name = input.get('file_name')
    endpoint = input.get('endpoint') 
    exec_uuid = input.get('uuid')

    #Save in container raw
    container_name = "raw"
    account_url = f"https://{account_name}.blob.core.windows.net"
    local_file_path = f'/tmp/{endpoint}/{exec_uuid}/{file_name}.json'

    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
    container_client = blob_service_client.get_container_client(container_name)
                
    relative_path = local_file_path.replace("/tmp", "")
    blob_path = storage_path + relative_path
    blob_path = blob_path.replace("\\", "/")
    
    blob_client = container_client.get_blob_client(blob_path)
    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    logging.info(f"Arquivo '{local_file_path}' carregado como '{blob_path}'")