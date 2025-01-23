import json
import os
import requests
import datetime
import time
import logging

class ApiIGDBHandler:

    def __init__(self, client_id, client_secret) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = self.get_twitch_token()

        self.base_url = 'https://api.igdb.com/v4/{sufix}'

        self.headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self.token}",
        }

    def get_twitch_token(self):

        url = "https://id.twitch.tv/oauth2/token"

        params = {
            "client_id" : self.client_id,
            "client_secret" : self.client_secret,
            "grant_type" : "client_credentials",
            }

        resp = requests.post(url, params=params)
        data = resp.json()

        token = data['access_token']
        return token


    def get_data(self, sufix, body):

        max_retries=3
        url = self.base_url.format(sufix=sufix)

        for attempt in range(1, max_retries + 1):
            try:
                data = requests.post(url, headers=self.headers, data=body)
                return data.json()
            except requests.RequestException as e:
                logging.info(f"Tentativa {attempt} falhou: {e}")
                if attempt < max_retries:
                    time.sleep(2)
                else:
                    logging.info("Número máximo de tentativas atingido. Falha na operação.")
                    raise
    
    def save_data_tmp(self, sufix, exec_uuid, data, file_name):

        logging.info(f'Salvando em /tmp/{sufix}/{exec_uuid}/{file_name}.json')

        os.makedirs(f'/tmp/{sufix}/{exec_uuid}', exist_ok=True)
        with open(f'/tmp/{sufix}/{exec_uuid}/{file_name}.json', 'w') as open_file:
            json.dump(data, open_file)
        return True

    def get_and_save_tmp(self, sufix, exec_uuid, body, file_name):
        data = self.get_data(sufix, body)
        self.save_data_tmp(sufix, exec_uuid, data, file_name)
        return data

    def process(self, sufix, exec_uuid, extract_type, delta_days=1, offset=0, file_name = None):

        if extract_type == 'full':
            body = f"""fields *;
                limit 500;
                offset {offset};
                sort updated_at asc;"""

            data = self.get_and_save_tmp(sufix, exec_uuid, body, file_name)

            if len(data) < 500:
                return False
            else:
                return True

        elif extract_type == 'delta':
            delta_timestamp = datetime.datetime.now() - datetime.timedelta(days=delta_days)
            delta_timestamp = int(delta_timestamp.timestamp())
            
            body = f"""fields *;
                limit 500;
                offset {offset};
                where updated_at >= {delta_timestamp};
                sort updated_at asc;"""
            logging.info("Obtendo dados...")
            data = self.get_and_save_tmp(sufix, exec_uuid, body, file_name)

            if len(data) < 500:
                return False
            else:
                return True


