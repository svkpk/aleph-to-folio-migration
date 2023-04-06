import sys, os, requests, json, argparse
import config

# pretty print; debug
import pprint
pp = pprint.PrettyPrinter(indent=2)

parser=argparse.ArgumentParser(
    description = "Promazání záznamů modulu inventáře. Scriptem je možné vymazat záznamy vytvořené migračním nástrojem (prochází se seznam instancí v souboru instance_id_map.json), nebo vlastní seznam instancí identifikovaných UUID (prochází se vlastní textový soubor; jeden identifikátor UUID na řádek). Script pro jednotlivá UUID záznamu instancí dohledává podřízené záznamy holdingů, jednotek a SRS záznamů instancí, které následně vymaže voláním API modulu mod-inventory."
)
parser.add_argument("--mode", help="Rezim --mode=\"map\" prochazi soubor instance_id_map.json vytvoreny migracnim nastrojem. \nRezim --mode=\"list\" prochazi soubor definovany v parametru --listfile. Vychozi rezim je \"map\"")
parser.add_argument("--iteration", help="V pripade volby rezimu \"map\" se pouzije adresar s cislem definovanym v parametru --iteration. Pokud je zadane cislo 1, pouzije se adresar run_001. Vychozi hodnota je 1.")
parser.add_argument("--listfile", help="V pripade volby rezimu \"list\" se pouzije seznam identifikatou UUID instanci ulozenych v tomto souboru. Jednotlive UUID vkladat jeden na jeden radek.")
args=parser.parse_args()
argsDict = vars(args)

# mode
mode = 'map'

# cislo v nazvu podadresare v adresari iterations
iteration = 1

# nazev souboru seznamu s UUID zaznamu instanci k vymazani
listfile = ""

try:
    if 'mode' in argsDict and argsDict['mode'] != None:
        mode = argsDict['mode']
    if 'iteration' in argsDict and argsDict['iteration'] != None:
        iteration = int(argsDict['iteration'])
    if 'listfile' in argsDict and argsDict['listfile'] != None:
        listfile = argsDict['listfile']
except Exception as err:
    print(err)
    sys.exit(0)

iterationDir = 'run_' + str(iteration).zfill(3)

if mode != 'map' and mode != 'list':
    print('Podporovane rezimy jsou "map" a "list"')
    sys.exit(0)

if mode != 'list' and listfile != '':
    print('Neni definovan soubor se seznamem UUID instanci k vymazani. Pouzijte parametr --listfile')
    sys.exit(0)


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

    headersAuth = {
        'Content-type': 'application/json',
        'X-Okapi-Tenant': config.FOLIO_TENANT
    }

    r = requests.post(url = config.OKAPI_LOGIN_URL, json = data, headers = headersAuth)

    if not r.ok:
        print(r.text)
        sys.exit()

    authJson = r.json()
    okapiToken = authJson['okapiToken']
    ftoken.write(okapiToken)
    ftoken.close()


# ------------------------------------------------------------------------------
#   Hlavicka OKAPI requestu pro authentizaci
# ------------------------------------------------------------------------------
headers = {
    'Content-type': 'application/json',
    'X-Okapi-Tenant': config.FOLIO_TENANT,
    'X-Okapi-Token': okapiToken
}


# vypis vsech zaznamu v SRS biblio storage
"""
rSRS = requests.get(url = config.OKAPI_URL+'/source-storage/records', headers = headers)
srsJson = rSRS.json()
pp.pprint(srsJson)
sys.exit()
"""


# ------------------------------------------------------------------------------
#   Funkce pro mazani jednoho zaznamu instance vcetne podrizenych zaznamu
#   Vyhledava podrizene (v tomto poradi): holdingy, jednotky, instance SRS
#   Maze postupne (v tomto poradi): jednotky, holdingy, instance SRS, instance
# ------------------------------------------------------------------------------

def removeInstance(instUUID):
    print("\u2759 Instance          [ " + instUUID + " ]")
    
    # vyhledej holdingy podrizene dane instance ke smazani
    rHold = requests.get(url = config.OKAPI_URL + '/holdings-storage/holdings?limit=1000&query=instanceId%3D%3D' + instUUID, headers = headers)
    
    if not rHold.ok:
        print(rHold.text)
        sys.exit()
    
    holdsJson = rHold.json()
    
    
    # predchozi dotaz vratil JSON objekt, ktery obsahuje podrizene holding zaznamy. Pripadne pole holdingu muze byt i prazdne.
    if holdsJson:
        cntHolds = 0
        for holdItem in holdsJson['holdingsRecords']:
            cntHolds += 1
            holdUUID = holdItem['id']
            print("  \u2759 Holding         [ " + holdUUID + " ]")
            
            # vyhledej jednotky daneho holdingu
            rItem = requests.get(url = config.OKAPI_URL + '/inventory/items?limit=50000&query=holdingsRecordId%3D%3D' + holdUUID, headers = headers)
            
            if not rItem.ok:
                print(rItem.text)
                sys.exit()
            
            itemsJson = rItem.json()
            
            
            # predchozi dotaz vratil JSON objekt, ktery obsahuje podrizene zaznamy jednotek. Pripadne pole jednotek muze byt i prazdne.
            if itemsJson:
                cntItems = 0
                for holdItem in itemsJson['items']:
                    cntItems += 1
                    itemUUID = holdItem['id']
                    print("    \u2715 DEL item      [ " + itemUUID + " ]")
                    
                    # ----------------
                    # Smazat jednotku
                    # ----------------
                    itemDelUrl = config.OKAPI_URL + '/inventory/items/' + itemUUID
                    r = requests.delete(url = itemDelUrl, headers = headers)
                    
                    if not r.ok:
                        print("\n\u274C \u274C \u274C\n" + r.text + "\nERR query: " + itemDelUrl + "\n")
                        sys.exit()
                
                if not cntItems:
                    print("      Neobsahuje jednotky")
            
            # ---------------
            # Smazat holding
            # ---------------
            holdDelUrl = config.OKAPI_URL + '/holdings-storage/holdings/' + holdUUID
            r = requests.delete(url = holdDelUrl, headers = headers)
            
            if r.ok:
                print("  \u2715 DEL holding     [ " + holdUUID + " ]")
            else:
                print("\n\u274C \u274C \u274C\n" + r.text + "\nERR query: " + holdDelUrl + "\n")
                sys.exit()
        
        if not cntHolds:
            print("    Neobsahuje holdingy")
    
    # ---------------------------------
    # Vyhledat instanci SRS ke smazani
    # ---------------------------------
    rSRS = requests.get(url = config.OKAPI_URL+'/source-storage/source-records?externalId=' + instUUID, headers = headers)
    
    if not rSRS.ok:
        print(rSRS.text)
        sys.exit()
    
    SRSjson = rSRS.json()
    
    if SRSjson:
        for srsItem in SRSjson['sourceRecords']:
            srsUUID = srsItem['recordId']
            print("\u2759 Instance SRS      [ " + srsUUID + " ]")
            
            # --------------------
            # Smazat instanci SRS
            # --------------------
            srsDelUrl = config.OKAPI_URL + '/source-storage/records/' + srsUUID
            r = requests.delete(url = srsDelUrl, headers = headers)
            
            if r.ok:
                print("\u2715 DEL instance SRS  [ " + srsUUID + " ]")
            else:
                print("\n\u274C \u274C \u274C\n" + r.text + "\nERR query: " + srsDelUrl + "\n")
                sys.exit()
    
    # ----------------
    # Smazat instanci
    # ----------------
    instDelUrl = config.OKAPI_URL + '/inventory/instances/' + instUUID
    r = requests.delete(url = instDelUrl, headers = headers)
    
    if r.ok:
        print("\u2715 DEL instance      [ " + instUUID + " ]")
    elif r.status_code == 404:
        print("    Instance neexistuje")
    else:
        print("\n\u274C \u274C \u274C\n" + r.text + "\nERR query: " + instDelUrl + "\n")
        sys.exit()


# ------------------------------------------------------------------------------
#   Rezim "map" prochazejici soubor instance_id_map.json
# ------------------------------------------------------------------------------

if mode == 'map':
    with open('iterations/' + iterationDir + '/results/instances_id_map.json', 'r') as f:
        cntInst = 0
        for instRow in f:
            cntInst += 1
            inst = json.loads(instRow)
            instUUID = inst['folio_id']
            instLegacyId = inst['legacy_id']
            #instUUID = '46bb6c82-6c01-597e-8948-be3d4ad4e547'
            print('#' + str(cntInst))
            print('instance legacy_id: ' + instLegacyId)
            removeInstance(instUUID)
        f.close()

# ------------------------------------------------------------------------------
#   Rezim "list" prochazejici soubor --listfile vstupniho parametru
# ------------------------------------------------------------------------------

elif mode == 'list' and listfile != '':
    with open(listfile, 'r') as f:
        cntInst = 0
        for line in f:
            cntInst += 1
            instUUID = line.strip()
            print('#' + str(cntInst))
            removeInstance(instUUID)
        f.close()
