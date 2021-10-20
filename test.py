from utils import api, json

data = api.get_api_response("/ladder/1")
print(json.dumps(data))
