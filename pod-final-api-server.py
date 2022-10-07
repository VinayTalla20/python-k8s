import os
import requests
import json

cert = os.getenv('CACERT')
print(cert)

token = os.getenv('TOKEN')
print(token)

apiserver = os.getenv('APISERVER')
print(apiserver)


s = requests.Session()

s.headers = {'authorization': f'Bearer {token}'}

url = f'{apiserver}/api/v1/namespaces/test/pods'
print(url)
s.verify = cert
ret = s.get( url)

assert ret.status_code == 200

for i in ret.json()['items']:
    print(i['metadata']['name'])
    
    
    
"""url = ('https://192.168.2.165:6443/api/v1/namespaces?watch=true')
s.verify = cert
ret = s.get( url, stream=True)

assert ret.status_code == 200

for i in ret.iter_lines():
       i = json.loads(i.decode('utf8'))
       evt, obj = i['type'], i['object']
       print(evt, obj['kind'], obj['metadata']['name'])"""

