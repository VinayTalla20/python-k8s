import os
import requests
import json

#client_url =  os.system(curl --cacert ${CACERT}  --header "Authorization: Bearer ${TOKEN}" https://192.168.2.165:6443/api)
#apiserver = https://kubernetes.default.svc.cluster.local
#url = https://kubernetes.default.svc.cluster.local/api/v1/namespaces
#url = 'https://192.168.2.165:6443/api'

cert = os.getenv('CACERT')
print(cert)

token = os.getenv('TOKEN')
print(token)

#headers = "Authorization: Bearer",token""

#print(headers)

 #token=open(fname_token, 'r').read(),

 #sess.headers = {'authorization': f'Bearer {config.token}'}

#url = 'https://192.168.2.165:6443/api --cacert'+cert

#output = requests.get( url, headers={'"Authorization': 'Bearer'+token})

#print(output)


"""def setup_requests(config: Config):
    # Configure a 'requests' session with the correct CA and pre-load the
    # Bearer token.
    sess = requests.Session()
    sess.verify = config.ca_cert

    if config.token is not None:
        sess.headers = {'authorization': f'Bearer {config.token}'}
    if config.client_cert is not None:
        sess.cert = (config.client_cert.crt, config.client_cert.key)
    return sess"""

s = requests.Session()

s.headers = {'authorization': f'Bearer {token}'}
#auth = print(token)
url = 'https://192.168.2.165:6443/api/v1/namespaces/test/pods'
s.verify = cert
ret = s.get( url)

assert ret.status_code == 200

for i in ret.json()['items']:
    print(i['metadata']['name'])
