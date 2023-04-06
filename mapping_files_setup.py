import sys, os, dbm
import requests
import config

argc = sys.argv
arg1 = ''
if len(argc) > 1: arg1 = sys.argv[1]
print(arg1)

OKAPI_MAPPING_URL = fn_mapping = fn_uuid = ''

if arg1 == 'loan_types':
    OKAPI_MAPPING_URL = config.OKAPI_LOAN_TYPES
    fn_mapping = config.fn_loan_types_mapping
    fn_uuid = config.fn_loan_types_uuid
elif arg1 == 'material_types':
    OKAPI_MAPPING_URL = config.OKAPI_MATERIAL_TYPES
    fn_mapping = config.fn_material_types_mapping
    fn_uuid = config.fn_material_types_uuid
elif arg1 == 'user_groups':
    OKAPI_MAPPING_URL = config.OKAPI_USER_GROUPS
    fn_mapping = config.fn_user_groups_mapping
    fn_uuid = config.fn_user_groups_uuid
elif arg1 == 'addr_types':
    OKAPI_MAPPING_URL = config.OKAPI_ADDR_TYPES
    fn_mapping = config.fn_addr_types_mapping
    fn_uuid = config.fn_addr_types_uuid
elif arg1 == 'feefine_types':
    OKAPI_MAPPING_URL = config.OKAPI_FEEFINE_TYPES
    fn_mapping = config.fn_feefine_types_mapping
    fn_uuid = config.fn_feefine_types_uuid
elif arg1 == 'service_points':
    OKAPI_MAPPING_URL = config.OKAPI_SERVICE_POINTS
    fn_mapping = config.fn_service_points_mapping
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
#   ALEPH: z30_item_status
# ------------------------------------------------------------------------------

fw = open(fn_uuid, 'w')
fr = open(fn_mapping, 'r')

if arg1 == 'service_points':
    # pomocna key-value db pro mapovani service_point_id -> uuid
    service_points_db = dbm.open(config.fn_service_points_uuid_db, 'c')

headers = {
    'Content-type': 'application/json',
    'X-Okapi-Tenant': config.FOLIO_TENANT,
    'X-Okapi-Token': okapiToken
}

i = 0
for line in fr:
    i += 1
    if i==1: continue # hlavicku souboru preskocit
    
    lineSplit = line.split("\t")
    alephCode = lineSplit[0]
    folioName = lineSplit[1]
    folioName = folioName[0:-1]
    
    if arg1 == 'loan_types':
        data = {
            'name': folioName
        }
    elif arg1 == 'material_types':
        data = {
            'name': folioName,
            'source': 'local'
        }
    elif arg1 == 'user_groups':
        data = {
            'group': folioName
        }
    elif arg1 == 'addr_types':
        data = {
            'addressType': folioName
        }
    elif arg1 == 'feefine_types':
        data = {
            'automatic': 'false',
			'feeFineType': folioName,
			'ownerId': config.ownerId
        }
    elif arg1 == 'service_points':
        data = {
            'name': folioName,
            'code': folioName,
            'discoveryDisplayName': folioName,
			'pickupLocation': 'true',
			'holdShelfExpiryPeriod': {
				'duration': 2,
				'intervalId': 'Days'
			}
        }
    
    r = requests.post(url = OKAPI_MAPPING_URL, json = data, headers = headers)
    
    if not r.ok:
        print(r.text)
        sys.exit()
    else:
        print(folioName)
    
    # ulozit do FOLIO
    respJson = r.json()
    uuid = respJson['id']
    
    # ulozit novo vytvoreny UUID pro moznost pozdejsiho odmazani, pokud by bylo potreba
    fw.write(uuid + "\n")
    
    if arg1 == 'service_points':
        # zapsat mapovani service_point -> uuid do pomocne db
        service_points_db[folioName] = uuid

fw.close()
fr.close()
if arg1 == 'service_points':
    service_points_db.close()

