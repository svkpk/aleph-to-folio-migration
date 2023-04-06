import sys
import os
import requests
import config

# ------------------------------------------------------------------------------
#   AUTH
# ------------------------------------------------------------------------------

okapiToken = ''

if os.path.exists(config.fn_okapi_token):
    ftoken = open(config.fn_okapi_token, 'r')
    okapiToken = ftoken.readline()
    ftoken.close()

if okapiToken=='':
    ftoken = open(config.fn_okapi_token, 'w')
    data = {
        'tenant': config.FOLIO_TENANT,
        'username': config.FOLIO_USERNAME,
        'password': config.FOLIO_USERPASS
    }

    headers = {
        'Content-type': 'application/json',
        'X-Okapi-Tenant': config.FOLIO_TENANT
    }

    r = requests.post(url = config.OKAPI_LOGIN_URL, json = data, headers = headers)

    if not r.ok:
        print(r.text)
        sys.exit()

    authJson = r.json()
    okapiToken = authJson['okapiToken']
    ftoken.write(okapiToken)
    ftoken.close()


# ------------------------------------------------------------------------------
#   SMAZAT FOLIO: Locations
# ------------------------------------------------------------------------------

f = open(config.fn_locations_uuid, 'r')

for uuid in f:
    uuid = uuid[:-1]
    print('Loc: ' + uuid)
    if len(uuid)<32: continue
    
    headers = {
        'Content-type': 'application/json',
        'X-Okapi-Tenant': config.FOLIO_TENANT,
        'X-Okapi-Token': okapiToken
    }
    
    r = requests.delete(url = config.OKAPI_LOCATIONS + '/' + uuid, headers = headers)
    
    if not r.ok:
        print(r.text)
        sys.exit()

f.close()
os.remove(config.fn_locations_uuid)


# ------------------------------------------------------------------------------
#   SMAZAT FOLIO: Libraries
# ------------------------------------------------------------------------------

f = open(config.fn_libraries_uuid, 'r')

for uuid in f:
    uuid = uuid[:-1]
    print('Lib: ' + uuid)
    if len(uuid)<32: continue
    
    headers = {
        'Content-type': 'application/json',
        'X-Okapi-Tenant': config.FOLIO_TENANT,
        'X-Okapi-Token': okapiToken
    }
    
    r = requests.delete(url = config.OKAPI_LIBRARIES_URL + '/' + uuid, headers = headers)
    
    if not r.ok:
        print(r.text)
        sys.exit()

f.close()
os.remove(config.fn_libraries_uuid)
