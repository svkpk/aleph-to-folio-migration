import sys
import os
import requests
import config

libList = {
    'PNA50': 'Výpůjční pult',
    'SVKPK': 'Fond',
    'SVKVV': 'Volný výběr',
    'SVKA': 'Fond - audio',
    'PRAHA': 'NK ČR',
    'SVKH': 'Fond HK',
    'ANGL': 'Fond AK',
    'ANGLA': 'Fond AK - audio',
    'NEME': 'Fond NK',
    'NEMEA': 'Fond NK - audio',
    'NEMEV': 'Fond NK - video',
    'RAKO': 'Fond RAK',
    'RAKOA': 'Fond RAK - audio',
    'ROMA': 'Fond ROK',
    'ROMAA': 'Fond ROK - audio',
    'SLEP': 'Slepecká knihovna',
    'AUDIO': 'Audio',
    'CDROM': 'CDROM',
    'VIDEO': 'Video',
    'DVD': 'DVD',
    'MVS': 'MVS',
    'ST_VS': 'Všeobecná studovna',
    'ST_RB': 'Regionálni bibliografie',
    'ST_2P': 'Speciální studovna',
    'ST_CT': 'Čítárna',
    'ST_HU': 'Hudební kabinet',
    'ST_HF': 'Badatelna',
    'ISAKV': 'Akvizice',
    'ISKAT': 'Katalogizace',
    'ISMVS': 'ISMVS',
    'ISOPR': 'Knihařská dílna',
    'ISVAZ': 'Vazačská dílna',
    'ISDIG': 'Digi. centrum'
}

libList['UNKNOWN'] = 'Nespecifikovaná'

locList = {
    'HLAV': 'Hlavní budova',
    'VOLNY': 'Volný výběr',
    'PRAHA': 'sklad Hostivař',
    'BOR2': 'Bory 2 / násl. den',
    'BUSOV': 'Bušovice / čtvrtek',
    'BOR16': 'Bory 16 / násl. den',
    'BOR17': 'Bory 17 / násl. den',
    'STOD': 'Stod / čtvrtek',
    'ANGL': 'Anglická knihovna',
    'NEME': 'Německá knihovna',
    'RAKO': 'Rakouská knihovna',
    'HISP': 'Hispánská knihovna',
    'FRAN': 'Francouzská knihovna',
    'ITAL': 'Italská knihovna',
    'SVYC': 'Švýcarská knihovna',
    'SLEP': 'Slepecká knihovna',
    'ODZKF': 'Příručka - ODZKF',
    'RED': 'Příručka - ředitelství',
    'REV': 'Příručka - revize',
    'EKO': 'Příručka - ekonomika',
    'MVS': 'Příručka - MVS',
    'OVOK': 'Příručka - OVOK',
    'IT': 'Příručka - IT',
    'REG': 'Příručka - region.bibliografie',
    'HIST': 'Příručka - hist.fondy',
    'VZAC': 'Historické fondy',
    'DEF': 'DEF / násl. den',
    'STUD': 'Příručka - všeob.studovna',
    'HUD': 'Příručka - hudeb.kabinet',
    'SPEC': 'Příručka - spec.studovna',
    'REST': 'Restaurátorská dílna',
    'CIT': 'Příručka - čítárna',
    'NAM': 'Příručka - náměstek',
    'EKNIH': 'E-kniha'
}

locList['UNKNOWN'] = 'Nespecifikovaná'

lib2loc = {
    'ANGL': ['ANGL'],
    'ANGLA': ['ANGL'],
    'AUDIO': ['SLEP'],
    'CDROM': ['SLEP'],
    'VIDEO': ['SLEP'],
    'DVD': ['SLEP'],
    'NEME': ['NEME'],
    'NEMEA': ['NEME'],
    'NEMEV': ['NEME'],
    'PRAHA': ['PRAHA'],
    'RAKO': ['RAKO'],
    'RAKOA': ['RAKO'],
    'ROMA': ['HISP', 'FRAN', 'ITAL', 'SVYC'],
    'ROMAA': ['HISP', 'FRAN', 'ITAL', 'SVYC'],
    'SLEP': ['SLEP'],
    'ST_2P': ['SPEC'],
    'ST_CT': ['CIT'],
    'ST_HF': ['HIST', 'VZAC'],
    'ST_HU': ['HUD'],
    'ST_RB': ['REG'],
    'ST_VS': ['STUD', 'BOR2', 'HLAV'],
    'ISVAZ': ['HLAV'],
    'SVKA': ['HLAV', 'BOR2', 'BUSOV', 'BOR16', 'BOR17', 'STOD', 'ODZKF', 'RED', 'REV', 'EKO', 'MVS', 'OVOK', 'IT', 'REG', 'STUD', 'HUD', 'SPEC', 'CIT', 'NAM', 'EKNIH'],
    'SVKH': ['HUD'],
    'SVKPK': ['BOR2', 'BUSOV', 'BOR16', 'BOR17', 'STOD', 'ODZKF', 'RED', 'REV', 'EKO', 'MVS', 'OVOK', 'IT', 'REG', 'HIST', 'VZAC', 'DEF', 'STUD', 'HUD', 'SPEC', 'REST', 'CIT', 'NAM', 'EKNIH', 'HLAV'],
    'SVKVV': ['VOLNY', 'EKNIH']
}

lib2loc['UNKNOWN'] = ['UNKNOWN']


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
#   ALEPH: z30_sub_library, FOLIO: libraries (3. uroven)
# ------------------------------------------------------------------------------

flib = open(config.fn_libraries_uuid, 'a')
floc = open(config.fn_locations_uuid, 'a')
floctsv = open(config.fn_locations_tsv, 'a')

# hlavicka tsv souboru - ciselniku pro migracni nastroj
floctsv.write("PERM_LOCATION\tfolio_code\n")

headers = {
    'Content-type': 'application/json',
    'X-Okapi-Tenant': config.FOLIO_TENANT,
    'X-Okapi-Token': okapiToken
}

for libKey in libList:
    libName = libList[libKey]
    
    data = {
        'name': libName,
        'code': libKey,
        'campusId': config.campusId
    }
    
    r = requests.post(url = config.OKAPI_LIBRARIES_URL, json = data, headers = headers)
    
    if not r.ok:
        print(r.text)
        sys.exit()
    else:
        print(libKey + ' : ' + libName)
    
    # ulozit do FOLIO
    respJson = r.json()
    libUUID = respJson['id']
    
    # ulozit novo vytvoreny UUID pro moznost pozdejsiho odmazani, pokud by bylo potreba
    flib.write(libUUID + "\n")
    
    
    # --------------------------------------------------------------------------
    #   ALEPH: z30_collection, FOLIO: locations (4. uroven)
    # --------------------------------------------------------------------------
    if libKey in lib2loc:
        
        #
        # KNIHOVNA MA LOKACI
        #
        for locKey in lib2loc[libKey]:
            locName = locList[locKey]
            newRecKey = libKey + '-' + locKey
            
            name = libName + ' - ' + locName
            if libName == locName: name = locName
            
            data = {
                'isActive': 'true',
                'institutionId': config.institutionId,
                'campusId': config.campusId,
                'libraryId': libUUID,
                'servicePointIds': config.servicePointList,
                'primaryServicePoint': config.primaryServicePoint,
                'name': name,
                'code': newRecKey,
                'discoveryDisplayName': name
            }
            
            r = requests.post(url = config.OKAPI_LOCATIONS, json = data, headers = headers)
            
            if not r.ok:
                print(r.text)
                sys.exit()
            else:
                print('>>> ' + newRecKey + ' : ' + name)
            
            # ulozit do FOLIO
            respJson = r.json()
            
            # ulozit novo vytvoreny UUID pro moznost pozdejsiho odmazani, pokud by bylo potreba
            floc.write(respJson['id'] + "\n")
            
            # do tsv souboru - ciselniku pro migracni nastroj
            floctsv.write(newRecKey + "\t" + newRecKey + "\n")

    else:
        
        #
        # KNIHOVNA NEMA LOKACI, vytvori se jedna default
        #
        data = {
            'isActive': 'true',
            'institutionId': config.institutionId,
            'campusId': config.campusId,
            'libraryId': libUUID,
            'servicePointIds': config.servicePointList,
            'primaryServicePoint': config.primaryServicePoint,
            'name': libName,
            'code': libKey,
            'discoveryDisplayName': libName
        }
        
        r = requests.post(url = config.OKAPI_LOCATIONS, json = data, headers = headers)
        
        if not r.ok:
            print(r.text)
            sys.exit()
        else:
            print('>>> ORPHAN : ' + libName)
        
        respJson = r.json()
        
        # ulozit novo vytvoreny UUID pro moznost pozdejsiho odmazani, pokud by bylo potreba
        floc.write(respJson['id'] + "\n")
        
        # do tsv souboru - ciselniku pro migracni nastroj
        floctsv.write(libKey + "\t" + libKey + "\n")


floctsv.write("*\tUNKNOWN\n")

flib.close()
floc.close()
floctsv.close()
