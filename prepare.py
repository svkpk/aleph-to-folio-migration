# interni v python
import sys, os, argparse, json, dbm
from datetime import datetime

# externi z pypi repozitare
from pymarc import marcxml
from oaipmh.client import Client # https://infrae.com/download/oaipmh
from oaipmh.metadata import MetadataRegistry
from lxml.etree import tostring
from io import BytesIO
import config

# pretty print; debug
import pprint
pp = pprint.PrettyPrinter(indent=2)

# cislo v nazvu podadresare v adresari iterations
iteration = 1

# max pocet zaznamu ktery se bude v OAI prochazet
argLimit = 100

# zaznamu v jedne davce
argBatchSize = 100

parser=argparse.ArgumentParser()
parser.add_argument("--from", help="OAI-PMH datum od - OAI-PMH parametr from. Priklad pouziti --from=\"2022-01-01T00:00:00Z\"")
parser.add_argument("--until", help="OAI-PMH datum do - OAI-PMH parametr until. Priklad pouziti --until=\"2022-02-28T00:00:00Z\"")
parser.add_argument("--limit", help="Max pocet zaznamu ke zpracovani. Default = " + str(argLimit))
parser.add_argument("--batchSize", help="Pocet bibliografickych zaznamu v jednem vystupnim souboru. Default = " + str(argBatchSize))
parser.add_argument("--iteration", help="Identifikator podadresare v adresari iterations pro ukladani vystupnich souboru. Pokud je zadane cislo 1, pouzije se adresar run_001. Vychozi hodnota je 1.")
parser.add_argument("--holdCallNumber", help="Zapnout generovani signatur holdingovych zaznamu. Default = 0 (vypnute)")
args=parser.parse_args()
argsDict = vars(args)

argFrom = argUntil = ''
argHoldCallNumber = False
try:
    if 'from' in argsDict and argsDict['from'] != None:
        argFrom = datetime.strptime(argsDict['from'], '%Y-%m-%dT%H:%M:%SZ')
    if 'until' in argsDict and argsDict['until'] != None:
        argUntil = datetime.strptime(argsDict['until'], '%Y-%m-%dT%H:%M:%SZ')
    if 'limit' in argsDict and argsDict['limit'] != None:
        argLimit = int(argsDict['limit'])
    if 'iteration' in argsDict and argsDict['iteration'] != None:
        iteration = int(argsDict['iteration'])
    if 'batchSize' in argsDict and argsDict['batchSize'] != None:
        argBatchSize = int(argsDict['batchSize'])
    if 'holdCallNumber' in argsDict and argsDict['holdCallNumber'] != None:
        if argsDict['holdCallNumber']=='1': argHoldCallNumber = True
except Exception as err:
    print(err)
    sys.exit(0)

iterationDir = 'run_' + str(iteration).zfill(3)

print("---------")
print('Bude zpracovano ' + str(argLimit) + ' bibliografickych zaznamu v davce po ' + str(argBatchSize) + ' zaznamech')

# --------------------------------------------------------------------------
#  Spojeni na zdrojovy OAI-PMH
#  https://aleph.svkpk.cz/OAI?verb=ListRecords&Set=PNA01-NEWILS-MARC21&metadataPrefix=marc21
# --------------------------------------------------------------------------
OAI_url = config.OAI_url
OAI_set = config.OAI_set
OAI_meta_prefix = config.OAI_meta_prefix


batchFilePostfix = str(1).zfill(4)
print('Vytvarim vystupni soubory se sekvencnim cislem ' + batchFilePostfix)

# vystupni soubor pro ukladani dat katalogu, .mrc file, write, byte mode
catmrc = open('iterations/' + iterationDir + '/source_data/instances/bibs_' + batchFilePostfix + '.mrc', 'wb')

# vystupni soubor pro holdingy; .tsv file
holdtsv = open('iterations/' + iterationDir + '/source_data/items/holdings_' + batchFilePostfix + '.tsv', 'w')

# vystupni soubor pro jednotky, .tsv file
itemstsv = open('iterations/' + iterationDir + '/source_data/items/items_' + batchFilePostfix + '.tsv', 'w')

# pomocna key-value db pro mapovani item_id -> barcode
itemid_barcode_db = dbm.open(config.fn_itemid_barcode_db, 'c')

# seznam sloupcu .tsv souboru, pro zaznamy jednotek
itemsColList = [
    'Z30_REC_KEY',
    'fake_instance_id',
    'Z30_BARCODE',
    'Z30_CALL_NO',
    'Z30_CALL_NO_2',
    'Z30_DESCRIPTION',
    'Z30_ENUMERATION_A',
    'Z30_ENUMERATION_B',
    'Z30_CHRONOLOGICAL_I',
    'Z30_CHRONOLOGICAL_J',
    'Z30_CHRONOLOGICAL_K',
    'Z30_SUB_LIBRARY',
    'Z30_COLLECTION',
    'Z30_ITEM_STATUS',
    'Z30_NOTE_INTERNAL',
    'Z30_DOC_NUMBER',
    'Z30_ITEM_PROCESS_STATUS',
    'Z30_ITEM_SEQUENCE',
    'Z30_NOTE_OPAC',
    'Z30_NOTE_CIRCULATION',
    'Z30_COPY_ID',
    'Z30_INVENTORY_NUMBER',
    'Z30_LAST_SCHELF_REPORT_DATE',
    'Z30_OPEN_DATE',
    'Z30_CALL_NO_TYPE',
    'Z30_MATERIAL',
    'holdings_note',
    'PERM_LOCATION'
]

# seznam sloupcu .tsv souboru, pro zaznamy holdingu
holdColList = [
    'Z30_REC_KEY',
    'fake_instance_id',
    'SUPPRESS_IN_OPAC',
    'holdings_note',
    'PERM_LOCATION',
    'SIGNATURA'
]


def writeFileHeaders(f1, f2):
    # prvni radek souboru jednotek, hlavicka souboru
    fileHeader = ''
    i = 0
    for colName in itemsColList:
        if i: fileHeader = fileHeader + "\t" # oddelovac polozek je tabelator
        fileHeader = fileHeader + colName
        i=i+1
    f1.write(fileHeader)

    # prvni radek souboru holdingu, hlavicka holdingu
    fileHeader = ''
    i = 0
    for colName in holdColList:
        if i: fileHeader = fileHeader + "\t" # oddelovac polozek je tabelator
        fileHeader = fileHeader + colName
        i=i+1
    f2.write(fileHeader)

# zapis hlavicky souboru
writeFileHeaders(holdtsv, itemstsv)


# ------------------------------------------------------------------------------
#   ZDROJ DAT PRO KONVERZI: OAI-PMH
# ------------------------------------------------------------------------------

# definice metadata readera
class MARCXMLReader(object):
    # vracia PyMARC zaznam z OAI struktury
    def __call__(self, element):
        handler = marcxml.XmlHandler()
        marcxml.parse_xml( BytesIO( tostring(element[0]) ), handler)
        return handler.records[0]

# nastaveni OAI klienta
marcxml_reader = MARCXMLReader()
registry = MetadataRegistry()
registry.registerReader('marc21', marcxml_reader)
client = Client(OAI_url, registry)

# dotaz na OAI server
oaiListRecParams = {}
oaiListRecParams['metadataPrefix'] = OAI_meta_prefix
oaiListRecParams['set'] = OAI_set
if argFrom != '': oaiListRecParams['from_'] = argFrom
if argUntil != '': oaiListRecParams['until'] = argUntil
print('Parametry volani OAI-PMH: ')
for argKey in oaiListRecParams: print('  ' + argKey + ' = ' + str(oaiListRecParams[argKey]))
print("---------\n")

try:
    recs = client.listRecords( **oaiListRecParams )
except Exception as err:
    print(err)
    sys.exit(0)


i = 0
for rec in recs:
    recMarc = rec[1]
    tag001X = recMarc.get_fields('001')
    tag001 = tag001X[0].format_field()
    #print(tag001)
    
    if str(type(recMarc)) != "<class 'pymarc.record.Record'>": continue;
    try:
        recJson = recMarc.as_dict() # JSON
    except:
        print("OAI recMarc.as_dict problem, 001:" + tag001)
    
    fld = recJson['fields']
    
    # 001
    tag001X = [x for x in fld if '001' in x]
    tag001 = tag001X[0]['001']
    #if tag001!='000000021': continue #debug
    
    # promenna pro text chyby, pokud se v prubehu zpracovani vnorenych cyklu objevi
    err = ''
    
    # vytvor pole vsech opakovani tagu 996
    tag996X = [x for x in fld if '996' in x]
    
    # odstranit 996, holdingy, neni je potrebne zapisovat do katalogoveho zaznamu
    # tady je mozne vsechna opakovani 996 odstrani, protoze jsou uz zalohovana v promenne tag996X
    if tag996X: recMarc.remove_fields('996')
    
    # debug
    #print(json.dumps(recJson, indent=2))
    #print(recMarc)
    
    # --------------------------------------------------------------------------
    #   MARC ISO 2790 katalogoveho zaznamu
    #   z ktereho bude vytvorena FOLIO instance
    # --------------------------------------------------------------------------
    catmrc.write(recMarc.as_marc())
    
    # --------------------------------------------------------------------------
    #   JEDNOTKY
    # --------------------------------------------------------------------------
    if tag996X:
        # ukolem je projich vsechna opakovani tagu 996 a vytvorit zaznam holdingu a jednotky
        # holding je agregaci lokace
        
        # slovnik pro ulozeni dat vsech poli 996, inicializace
        t996X = {}
        
        # ulozeni signatur 996c do agregovaneho slovniku obsahujiciho tyto urovne 996l + 996r + 996x
        signatury = {}
        
        # cyklus pres vsechna opakovani 996
        for tag996 in tag996X:
            if err!='': break
            subtag996X = tag996['996']['subfields']
            
            # slovnik pro ulozeni jednoho tagu 996, inicializace
            t996 = {}
            
            # inicializace hodnot pro klice agregavaneho slovniku signatury
            t996l = t996r = t996x = t996c = t996w = t996u = ''
            
            # cyklus pres vsechny subtagy jednoho tagu 996
            for subtag996 in subtag996X:
                if err!='': break
                
                # subtagy
                for subtagKey in subtag996.keys():
                    subtagVal = subtag996[subtagKey]
                    t996[subtagKey] = subtagVal
                    
                    if subtagKey=='l':
                        t996l = subtagVal
                    elif subtagKey=='r':
                        t996r = subtagVal
                    elif subtagKey=='x':
                        t996x = subtagVal
                    elif subtagKey=='c':
                        t996c = subtagVal
                    elif subtagKey=='w':
                        t996w = subtagVal
                    elif subtagKey=='u':
                        t996u = subtagVal
            
            if t996l=='' or t996r=='' or t996x=='':
                err = 'Err: empty subtag 996l or 996r or 996x'
                break
            
            if t996w=='' or t996u=='':
                err = 'Err: empty subtag 996w or 996u, unable to construct primary key'
                break
            
            t996X[t996w + t996u] = t996
            
            # inicializace listu pro prvni uroven agregace; 996l
            if t996l not in signatury: signatury[t996l] = {}
            # inicializace listu pro druhou uroven agregace; 996r
            if t996r not in signatury[t996l]: signatury[t996l][t996r] = {}
            # inicializace listu pro treti uroven agregace; 996x
            if t996x not in signatury[t996l][t996r]: signatury[t996l][t996r][t996x] = []
            # pridani signatury do listu
            signatury[t996l][t996r][t996x].append([t996c, t996w, t996u])
        
        # --------------------------------------------------------------------------
        # Vsechny signatury tagu 996 sesbirany, vysledkem je napr.:
        # { 'SVKPK': { 'BOR16': { 'BOOK': [ ['391A21320-2/1b', '000000021', '000030'],
        #                                   ['391A21320-2/1e', '000000021', '000060'],
        #                                   ['391A21320-2/1f', '000000021', '000070'],
        #                                   ['391A21320-2/1d', '000000021', '000050'],
        #                                   ['391A21320-2/1a', '000000021', '000020'],
        #                                   ['391A21320-2/1c', '000000021', '000040']]},
        #               'BOR2': { 'BOOK': [ ['391A21320-2/1k', '000000021', '000100'],
        #                                   ['391A21320-2/1', '000000021', '000010'],
        #                                   ['391A21320-2/1l', '000000021', '000110'],
        #                                   ['391A21320-2/1h', '000000021', '000090'],
        #                                   ['391A21320-2/1g', '000000021', '000080']]}}}
        #
        # --------------------------------------------------------------------------
        #
        # Dalsi kod bude slouzit k prirazeni identifikatoru a signatury pro 
        # FOLIO zaznamy na holding urovni.
        #
        # --------------------------------------------------------------------------
        
        # iterator identifikatoru holdingu ; postfix k t001 katalogoveho zaznamu
        holdIterator = 1
        
        # iterator signatur ; postfix k signature
        signIterator = 1
        
        # slovnik signatur Aleph jednotek a jim prirazeny holding identifikator a holding signatura
        # klic je aleph signatura
        # hodnota je list; idx 0 = identifikator holdingu; idx 1 = signatura holdingu
        signAleph = {}
        
        # list uz pridelenych holding signatur
        # klic je folio holding signatura
        # hodnota je holdIterator
        signFolio = {}
        
        # prirazeni holding identifikatoru a holding signatur
        for t996l in signatury:
            for t996r in signatury[t996l]:
                for t996x in signatury[t996l][t996r]:
                    signList = signatury[t996l][t996r][t996x]
                    
                    # pokud je jedina jednotka na dane lokaci a daneho typu dokumentu
                    # ponechej signaturu holdingu stejnou jako je signatura jednotky
                    if len(signList) == 1:
                        item0 = signatury[t996l][t996r][t996x][0]
                        sign0 = item0[0]
                        subtag996w = item0[1]
                        subtag996u = item0[2]
                        signAleph[subtag996w + subtag996u] = [tag001 + '_' + str(holdIterator).zfill(4), sign0]
                        signFolio[sign0] = holdIterator
                        holdIterator += 1
                    
                    else:
                        # vysledek po setrizeni:
                        # pro 'BOR2' list  [ '391A21320-2/1', '391A21320-2/1k', '391A21320-2/1l', '391A21320-2/1h', '391A21320-2/1g']
                        # pro 'BOR16' list [ '391A21320-2/1b', '391A21320-2/1e', '391A21320-2/1f', '391A21320-2/1d', '391A21320-2/1a', '391A21320-2/1c']
                        signaturySorted = sorted(signList, key=lambda x: len(x[0]))
                        
                        # zjisti miru shody prvnich dvou nejkratsich signatur
                        sign1 = signaturySorted[0][0]
                        sign2 = signaturySorted[1][0]
                        signMatchIdx = 0
                        for charIdx in range(0, len(sign1)):
                            if sign1[0:charIdx+1]==sign2[0:charIdx+1]: signMatchIdx=charIdx
                        
                        # mira shody je mala, nedosahuje 50% ; jako holding signaturu pouzij signaturu jednotky
                        if signMatchIdx < len(sign1)/2:
                            signMatchIdx = 0
                        
                        # inicializace promenne pro text signatury
                        signHold = ''
                        
                        # mira shody je vyssi nez 50% ; dostacujici ; dojde ke slouceni vicerych jednotek pod jednu signaturu holdingu
                        if signMatchIdx > 0:
                            signHold = sign1[0:signMatchIdx+1]
                            # pokud uz tato signatura holdingu byla pouzita navys iterator ; bude pripojen jako postfix
                            if signHold in signFolio: signIterator += 1
                            # pridani do seznamu uz pouzitych signatur
                            signFolio[signHold] = holdIterator
                            holdIterator += 1
                        
                        for item in signaturySorted:
                            sign = item[0]
                            subtag996w = item[1]
                            subtag996u = item[2]
                            if signHold != '' and signHold == sign[0:signMatchIdx+1]:
                                tmpHoldIterator = signFolio[signHold]
                                
                                # signHold se shoduje, pouzije se agregovana signatura
                                signAleph[subtag996w + subtag996u] = [tag001 + '_' + str(tmpHoldIterator).zfill(4), signHold]
                                # modifikace s pripojenim iteratoru s cilem aby vznikla unikatni signatura holding zaznamu
                                #signAleph[subtag996w + subtag996u] = [tag001 + '_' + str(tmpHoldIterator).zfill(4), signHold + '_' + str(signIterator).zfill(4)]
                            
                            else:
                                # signHold se neshoduje, pouzije se signatura jednotky
                                signAleph[subtag996w + subtag996u] = [tag001 + '_' + str(holdIterator).zfill(4), sign]
                                signFolio[sign] = holdIterator
                                holdIterator += 1
        
        #pp.pprint(signAleph)
        #print("\n--------------------------------\n")
        #pp.pprint(signFolio)
        #print("\n--------------------------------\n")
        
        if err!='':
            print("\n" + err)
            print(tag996X)
        
        
        # Mapovani Aleph poli na subtagy pole 996
        # z30-barcode                  b
        # z30-call-no                  c
        # z30-description              d
        # z30-enumeration-a            v
        # z30-enumeration-b            i
        # z30-chronological-i          y
        # z30-sub-library              l
        # z30-collection               r
        # z30-item-status              s
        # z30-note-internal            p
        # z30-call-no-2                h
        # z30-doc-number               w
        # z30-item-process-status      z
        # z30-item-sequence            u
        # z30-chronological-j          j
        # z30-chronological-k          k
        # z30-note-opac                o
        # z30-note-circulation         a
        # z30-copy-id                  f
        # z30-inventory-number         m
        # z30_last_shelf_report_date   e
        # z30-open-date                n
        # z30-call-no-type             t
        # z30-material                 x
        
        # --------------------------------------------------------------------------
        # Zapis do souboru holdingu
        # --------------------------------------------------------------------------
        usedHoldList = [] # list pro kontrolu jestli byl uz holding pouzity
        # cyklus pres vsechny jednotky z kterych se ziskaji unikatni vyskyty holdingu
        for itemT001 in signAleph:
            item = signAleph[itemT001]
            holdT001 = item[0]
            if holdT001 in usedHoldList: continue
            
            fileRow = ''
            j = 0
            for colName in holdColList:
                if j: fileRow += "\t" # oddelovac polozek je tabelator
                colVal = ''
                
                if colName == 'Z30_REC_KEY':
                    colVal = holdT001
                elif 'w' in t996X[itemT001] and colName == 'fake_instance_id':
                    colVal = t996X[itemT001]['w']
                elif colName == 'SUPPRESS_IN_OPAC':
                    colVal = '0'
                elif colName == 'holdings_note':
                    colVal = ''
                elif colName == 'PERM_LOCATION' and 'l' in t996X[itemT001] and 'r' in t996X[itemT001]:
                    colVal = t996X[itemT001]['l'] + '-' + t996X[itemT001]['r']
                elif colName == 'PERM_LOCATION' and 'l' in t996X[itemT001]:
                    colVal = t996X[itemT001]['l']
                elif colName == 'SIGNATURA':
                    if argHoldCallNumber: colVal = item[1]
                
                fileRow += colVal
                j += 1
            
            # pridat do listu aby nedoslo k opakovanemu pouziti stejneho holdingu
            usedHoldList.append(holdT001)
            # zapis do holdingoveho vystupniho souboru
            holdtsv.write("\n" + fileRow)
        
        # --------------------------------------------------------------------------
        # Zapis do souboru jednotek
        # --------------------------------------------------------------------------
        
        # cyklus pres vsechny jednotky Aleph
        for itemT001 in signAleph:
            item = signAleph[itemT001]
            holdT001 = item[0]
            
            fileRow = ''
            j = 0
            for colName in itemsColList:
                if j: fileRow += "\t" # oddelovac polozek je tabelator
                
                item = t996X[itemT001]
                colVal = ''
                
                if colName == 'Z30_REC_KEY':
                    colVal = itemT001
                elif colName == 'fake_instance_id':
                    colVal = holdT001
                elif 'b' in item and colName == 'Z30_BARCODE':
                    colVal = item['b']
                    # mapovani item_id -> barcode
                    itemid_barcode_db[itemT001] = colVal
                elif 'c' in item and colName == 'Z30_CALL_NO':
                    colVal = item['c']
                elif 'h' in item and colName == 'Z30_CALL_NO_2':
                    colVal = item['h']
                elif 'd' in item and colName == 'Z30_DESCRIPTION':
                    colVal = item['d']
                elif 'v' in item and colName == 'Z30_ENUMERATION_A':
                    colVal = item['v']
                elif 'i' in item and colName == 'Z30_ENUMERATION_B':
                    colVal = item['i']
                elif 'y' in item and colName == 'Z30_CHRONOLOGICAL_I':
                    colVal = item['y']
                elif 'j' in item and colName == 'Z30_CHRONOLOGICAL_J':
                    colVal = item['j']
                elif 'k' in item and colName == 'Z30_CHRONOLOGICAL_K':
                    colVal = item['k']
                elif 'l' in item and colName == 'Z30_SUB_LIBRARY':
                    colVal = item['l']
                elif 'r' in item and colName == 'Z30_COLLECTION':
                    colVal = item['r']
                elif 's' in item and colName == 'Z30_ITEM_STATUS':
                    colVal = item['s']
                elif 'p' in item and colName == 'Z30_NOTE_INTERNAL':
                    colVal = item['p']
                elif 'w' in item and colName == 'Z30_DOC_NUMBER':
                    colVal = item['w']
                elif 'z' in item and colName == 'Z30_ITEM_PROCESS_STATUS':
                    colVal = item['z']
                elif 'u' in item and colName == 'Z30_ITEM_SEQUENCE':
                    colVal = item['u']
                elif 'o' in item and colName == 'Z30_NOTE_OPAC':
                    colVal = item['o']
                elif 'a' in item and colName == 'Z30_NOTE_CIRCULATION':
                    colVal = item['a']
                elif 'f' in item and colName == 'Z30_COPY_ID':
                    colVal = item['f']
                elif 'm' in item and colName == 'Z30_INVENTORY_NUMBER':
                    colVal = item['m']
                elif 'e' in item and colName == 'Z30_LAST_SCHELF_REPORT_DATE':
                    colVal = item['e']
                elif 'n' in item and colName == 'Z30_OPEN_DATE':
                    colVal = item['n']
                elif 't' in item and colName == 'Z30_CALL_NO_TYPE':
                    colVal = item['t']
                elif 'x' in item and colName == 'Z30_MATERIAL':
                    colVal = item['x']
                elif colName == 'holdings_note':
                    colVal = ''
                elif colName == 'PERM_LOCATION' and 'l' in t996X[itemT001] and 'r' in t996X[itemT001]:
                    colVal = t996X[itemT001]['l'] + '-' + t996X[itemT001]['r']
                elif colName == 'PERM_LOCATION' and 'l' in t996X[itemT001]:
                    colVal = t996X[itemT001]['l']
                
                fileRow += colVal
                j += 1
            
            # zapis do holdingoveho vystupniho souboru
            itemstsv.write("\n" + fileRow)
    
    i += 1
    if not (i % 100): print(i)
    if argLimit>0 and i>=argLimit: break
    
    # limit zaznamu na jeden soubor dosazen, pokracuj dalsim souborem
    if i>0 and not (i % argBatchSize):
        catmrc.close()
        holdtsv.close()
        itemstsv.close()
        batchFileNo = (i // argBatchSize) + 1
        batchFilePostfix = str(batchFileNo).zfill(4)
        print('Vytvarim vystupni soubory se sekvencnim cislem ' + batchFilePostfix)
        catmrc = open('iterations/' + iterationDir + '/source_data/instances/bibs_' + batchFilePostfix + '.mrc', 'wb')
        holdtsv = open('iterations/' + iterationDir + '/source_data/items/holdings_' + batchFilePostfix + '.tsv', 'w')
        itemstsv = open('iterations/' + iterationDir + '/source_data/items/items_' + batchFilePostfix + '.tsv', 'w')
        writeFileHeaders(holdtsv, itemstsv)

catmrc.close()
holdtsv.close()
itemstsv.close()
