import requests
import json
from src.data_ingestion.utils.globals import global_configuration


class APIClient:

    def __init__(self):
        self.base_url = global_configuration()['api']['base_url']
        self.size = global_configuration()['api']['size']

    def get_data(self, endpoint: str, page: int = 1) -> json:
        url = f"{self.base_url}/{endpoint}"
        params = {
            'page': page,
            'size': self.size
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
