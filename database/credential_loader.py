import json


def load_credentials(database: str, fpath: str = r'.\credentials.json'):
    with open(fpath, 'r') as credfile:
        return json.load(credfile)['databases'][database]
