import requests

def get_proxy():
    return requests.get("http://127.0.0.1:5010/get/").content

def delete_proxy(proxy):
    requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy))

def proxy_status():
    return json.loads(requests.get("http://127.0.0.1:5010/get_status/").content)
