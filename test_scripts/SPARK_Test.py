import requests

URI = r'http://localhost:8001'

response = requests.post(URI, json={'scid': '1', 'start_time': "67",  'metric': 'PARS', 'source': 'ECQL', 'job_id': '1', 'endpoint': ''})

