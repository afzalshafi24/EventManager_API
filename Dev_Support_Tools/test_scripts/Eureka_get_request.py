import xmltodict
import requests

r = requests.get("http://localhost:8761/eureka/apps/EVENT-MGR-API")
content_dict = xmltodict.parse(r._content)

print(content_dict['application']['instance']['ipAddr'])
