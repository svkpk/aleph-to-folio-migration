import sys
import os
import requests
import config

argc = sys.argv
arg1 = ''
if len(argc) > 1: arg1 = sys.argv[1]
print(arg1)

OKAPI_MAPPING_URL = fn_mapping = fn_uuid = ''

if arg1 == 'loan_types':
    OKAPI_MAPPING_URL = config.OKAPI_LOAN_TYPES
    fn_uuid = config.fn_loan_types_uuid
elif arg1 == 'material_types':
    OKAPI_MAPPING_URL = config.OKAPI_MATERIAL_TYPES
    fn_uuid = config.fn_material_types_uuid
elif arg1 == 'user_groups':
    OKAPI_MAPPING_URL = config.OKAPI_USER_GROUPS
    fn_uuid = config.fn_user_groups_uuid
elif arg1 == 'addr_types':
    OKAPI_MAPPING_URL = config.OKAPI_ADDR_TYPES
    fn_uuid = config.fn_addr_types_uuid
elif arg1 == 'feefine_types':
    OKAPI_MAPPING_URL = config.OKAPI_FEEFINE_TYPES
    fn_uuid = config.fn_feefine_types_uuid
elif arg1 == 'service_points':
    OKAPI_MAPPING_URL = config.OKAPI_SERVICE_POINTS
    fn_uuid = config.fn_service_points_uuid
else:
    sys.exit()

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
#   SMAZAT
# ------------------------------------------------------------------------------

f = open(fn_uuid, 'r')

headers = {
    'Content-type': 'application/json',
    'X-Okapi-Tenant': config.FOLIO_TENANT,
    'X-Okapi-Token': okapiToken
}

for uuid in f:
    uuid = uuid[:-1]
    print(uuid)
    if len(uuid)<32: continue
    
    r = requests.delete(url = OKAPI_MAPPING_URL + '/' + uuid, headers = headers)
    
    if not r.ok:
        print(r.text)
        sys.exit()

f.close()
os.remove(fn_uuid)
