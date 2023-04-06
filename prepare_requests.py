# interni moduly
import sys, os, argparse
import csv, dbm, json
from datetime import datetime

# konfiguracni parametry
import config

# pocet zaznamu v jedne davce
argBatchSize = 100
# cislo v nazvu podadresare v adresari iterations
iteration = 1

parser=argparse.ArgumentParser()
parser.add_argument("--batchSize", help="Pocet zaznamu ctenaru v jednom vystupnim souboru. Default = " + str(argBatchSize))
parser.add_argument("--iteration", help="Identifikator podadresare v adresari iterations pro ukladani vystupnich souboru. Pokud je zadane cislo 1, pouzije se adresar run_001. Vychozi hodnota je 1.")
args=parser.parse_args()
argsDict = vars(args)
try:
    if 'batchSize' in argsDict and argsDict['batchSize'] != None:
        argBatchSize = int(argsDict['batchSize'])
    if 'iteration' in argsDict and argsDict['iteration'] != None:
        iteration = int(argsDict['iteration'])
except Exception as err:
    print(err)
    sys.exit(0)

iterationDir = 'run_' + str(iteration).zfill(3)
print("---------")
print('Zaznamy zadanek budou zpracovany v davce po ' + str(argBatchSize) + ' zaznamech')


# seznam sloupcu vstupniho csv pro zadanky
in_requests_cols = [
    'Z37_REC_KEY',
    'Z37_ID',
    'Z37_STATUS',
    'Z37_PRIORITY',
    'Z37_OPEN_DATE',
    'Z37_OPEN_HOUR',
    'Z37_END_REQUEST_DATE',
    'Z37_NOTE_1',
    'Z37_NOTE_2',
    'Z37_PICKUP_LOCATION',
    'Z37_SEND_ACTION'
]
# seznam sloupcu vystupniho tsv pro zadanky
out_requests_cols = [
    'item_barcode',
    'patron_barcode',
    'pickup_servicepoint_id',
    'request_date',
    'request_expiration_date',
    'comment',
    'request_type'
]

def write_headers(list_cols, out_tsv): 
    # prvni radek tsv souboru zadanek, hlavicka
    outline = []
    for col_name in list_cols:
        outline.append(col_name)
    out_tsv.writerow(outline)


# main program

# nacti soubor csv
csvfile = open('iterations/' + iterationDir + '/source_data/requests/' + config.fn_requests, newline='')
csv_in = csv.reader(csvfile, delimiter=";", quotechar='"')
print('Zdrojovy soubor ' + config.fn_requests + ' nacten')

# soubor pro ulozeni vysledku ve formatu tsv
batchFilePostfix = str(1).zfill(4)
print('Vytvarim vystupni soubor se sekvencnim cislem ' + batchFilePostfix)
requeststsv = open('iterations/' + iterationDir + '/source_data/requests/' + 'aleph_data_requests_' + batchFilePostfix + '.tsv', 'w', newline='', encoding="utf-8")
csvwriter = csv.writer(requeststsv, delimiter="\t")
# zapis hlavicky do vystupu
write_headers(out_requests_cols, csvwriter)

# pomocne key-value db pro mapovani item_id -> barcode, user_id -> barcode, service_point -> uuid
itemid_barcode_db = dbm.open(config.fn_itemid_barcode_db, 'r')
userid_barcode_db = dbm.open(config.fn_userid_barcode_db, 'r')
service_points_db = dbm.open(config.fn_service_points_uuid_db, 'r')

# prochazet vstupni csv z37, mapovat a zapisovat vybrane sloupce do vystupu
# nejdrive hlavicky
in_colnames = next(csv_in)
# datove radky
i = 0
for row in csv_in:
    i += 1
    outline = []
    wlog = not (i % 10)
    
    # cyklus pres vsechny sloupce vystupniho tsv
    for col_name in out_requests_cols:
        if col_name == 'item_barcode':
            # ziskani caroveho kodu jednotky
            item_id = row[in_colnames.index('Z37_REC_KEY')][0:15]
            ibarcode = itemid_barcode_db.get(item_id, b'').decode()
            outline.append(ibarcode)
        elif col_name == 'patron_barcode':
            # ziskani caroveho kodu ctenare
            user_id = row[in_colnames.index('Z37_ID')]
            ubarcode = userid_barcode_db.get(user_id, b'').decode()
            outline.append(ubarcode)
        elif col_name == 'pickup_servicepoint_id':
            # misto vyzvednuti
            pickup = row[in_colnames.index('Z37_PICKUP_LOCATION')]
            # prevest kod na UUID z ciselniku
            spuuid = service_points_db.get(pickup, b'').decode()
            outline.append(spuuid)
        elif col_name == 'request_date':
            # datum zadanky
            in_date = row[in_colnames.index('Z37_OPEN_DATE')]
            in_time = row[in_colnames.index('Z37_OPEN_HOUR')].zfill(4) + '00'
            in_dt = datetime.strptime(in_date + 'T' + in_time, '%Y%m%dT%H%M%S')
            outline.append(in_dt.isoformat(timespec='microseconds'))
        elif col_name == 'request_expiration_date':
            # datum platnosti zadanky
            in_date = row[in_colnames.index('Z37_END_REQUEST_DATE')]
            in_time = '235900'
            in_dt = datetime.strptime(in_date + 'T' + in_time, '%Y%m%dT%H%M%S')
            outline.append(in_dt.isoformat(timespec='microseconds'))
        elif col_name == 'comment':
            # poznamky
            note1 = row[in_colnames.index('Z37_NOTE_1')]
            note2 = row[in_colnames.index('Z37_NOTE_2')]
            outline.append('Migrace z Aleph.' + ' ' + note1 + ' ' + note2)
        elif col_name == 'request_type':
            # typ zadanky
            rtype = row[in_colnames.index('Z37_STATUS')]
            # Aleph ma stavy: A - Active (nova), S - hold Shelf (pripravena k vyzvednuti), W - hold Waiting (ceka na uvolneni jednotky)
            # Folio ma typy: Hold (jednotka je vypujcena, pockat na navrat), Recall (jednotka je vypujcena, urychlit navrat), Page (jednotka je volna)
            # pri migraci pro zjednoduseni pouzijeme jenom typ Hold, nevim jak jinak to namapovat
            outline.append('Hold')

    # zapis radku do vystupu
    #print('outline = ' + str(outline))
    csvwriter.writerow(outline)
    if wlog: print(str(i+1) + '. radek ze z37 zpracovan ...')
    
    if i>0 and not (i % argBatchSize):
        # limit poctu zaznamu na jeden soubor dosazen, pokracuj dalsim souborem
        requeststsv.close()
        print('Vystupni soubor aleph_data_requests_' + batchFilePostfix + '.tsv byl vytvoren')
        batchFileNo = (i // argBatchSize) + 1
        batchFilePostfix = str(batchFileNo).zfill(4)
        print('Vytvarim vystupni soubor se sekvencnim cislem ' + batchFilePostfix)
        requeststsv = open('iterations/' + iterationDir + '/source_data/requests/' + 'aleph_data_requests_' + batchFilePostfix + '.tsv', 'w', newline='', encoding="utf-8")
        csvwriter = csv.writer(requeststsv, delimiter="\t")
        # zapis hlavicky do vystupu
        write_headers(out_requests_cols, csvwriter)

csvfile.close()
requeststsv.close()
print('Vystupni soubor aleph_data_requests_' + batchFilePostfix + '.tsv byl vytvoren')
print('Hotovo')
