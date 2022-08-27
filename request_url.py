import requests
import json

def get_url_for_api(url):
    request_api = requests.get(url)
    data = request_api.text
    parse_json = json.loads(data)
    return parse_json