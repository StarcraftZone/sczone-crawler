import json
from utils import api

data = api.get_api_response("/ladder/1")
print(json.dumps(data))
