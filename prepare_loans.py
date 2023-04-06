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
print('Zaznamy vypujcek budou zpracovany v davce po ' + str(argBatchSize) + ' zaznamech')


# seznam sloupcu vstupniho csv pro vypujcky
in_loans_cols = [
    'Z36_REC_KEY',
    'Z36_ID',
    'Z36_SUB_LIBRARY',
    'Z36_STATUS',
    'Z36_LOAN_DATE',
    'Z36_LOAN_HOUR',
    'Z36_DUE_DATE',
    'Z36_DUE_HOUR',
    'Z36_ITEM_STATUS',
    'Z36_BOR_STATUS',
    'Z36_NO_RENEWAL'
]
# seznam sloupcu vystupniho tsv pro vypujcky
out_loans_cols = [
    'item_barcode',
    'patron_barcode',
    'due_date',
    'out_date',
    'renewal_count',
    'next_item_status'
]

def write_headers(list_cols, out_tsv): 
    # prvni radek tsv souboru vypujcek, hlavicka
    outline = []
    for col_name in list_cols:
        outline.append(col_name)
    out_tsv.writerow(outline)


# main program

# nacti soubor csv
csvfile = open('iterations/' + iterationDir + '/source_data/loans/' + config.fn_loans, newline='')
csv_in = csv.reader(csvfile, delimiter=";", quotechar='"')
print('Zdrojovy soubor ' + config.fn_loans + ' nacten')

# soubor pro ulozeni vysledku ve formatu tsv
batchFilePostfix = str(1).zfill(4)
print('Vytvarim vystupni soubor se sekvencnim cislem ' + batchFilePostfix)
loanstsv = open('iterations/' + iterationDir + '/source_data/loans/' + 'aleph_data_loans_' + batchFilePostfix + '.tsv', 'w', newline='', encoding="utf-8")
csvwriter = csv.writer(loanstsv, delimiter="\t")
# zapis hlavicky do vystupu
write_headers(out_loans_cols, csvwriter)

# pomocna key-value db pro mapovani item_id -> barcode a user_id -> barcode
itemid_barcode_db = dbm.open(config.fn_itemid_barcode_db, 'r')
userid_barcode_db = dbm.open(config.fn_userid_barcode_db, 'r')

# prochazet vstupni csv z36, mapovat a zapisovat vybrane sloupce do vystupu
# nejdrive hlavicky
in_colnames = next(csv_in)
# datove radky
i = 0
for row in csv_in:
    i += 1
    outline = []
    wlog = not (i % 10)
    
    # cyklus pres vsechny sloupce vystupniho tsv
    for col_name in out_loans_cols:
        if col_name == 'item_barcode':
            # ziskani caroveho kodu jednotky
            item_id = row[in_colnames.index('Z36_REC_KEY')]
            ibarcode = itemid_barcode_db.get(item_id, b'').decode()
            outline.append(ibarcode)
        elif col_name == 'patron_barcode':
            # ziskani caroveho kodu ctenare
            user_id = row[in_colnames.index('Z36_ID')]
            ubarcode = userid_barcode_db.get(user_id, b'').decode()
            outline.append(ubarcode)
        elif col_name == 'due_date':
            # datum do kdy se ma vratit
            in_date = row[in_colnames.index('Z36_DUE_DATE')]
            in_time = row[in_colnames.index('Z36_DUE_HOUR')].zfill(4) + '00'
            in_dt = datetime.strptime(in_date + 'T' + in_time, '%Y%m%dT%H%M%S')
            outline.append(in_dt.isoformat(timespec='microseconds'))
        elif col_name == 'out_date':
            # datum vypujcky
            in_date = row[in_colnames.index('Z36_LOAN_DATE')]
            in_time = row[in_colnames.index('Z36_LOAN_HOUR')]
            in_dt = datetime.strptime(in_date + 'T' + in_time, '%Y%m%dT%H%M%S')
            outline.append(in_dt.isoformat(timespec='microseconds'))
        elif col_name == 'renewal_count':
            # pocet prolongaci
            nprolong = row[in_colnames.index('Z36_NO_RENEWAL')]
            outline.append(nprolong)
        elif col_name == 'next_item_status':
            # budouci stav jednotky, nechame prazdny
            outline.append('')
    
    # zapis radku do vystupu
    #print('outline = ' + str(outline))
    csvwriter.writerow(outline)
    if wlog: print(str(i+1) + '. radek ze z36 zpracovan ...')
    
    if i>0 and not (i % argBatchSize):
        # limit poctu zaznamu na jeden soubor dosazen, pokracuj dalsim souborem
        loanstsv.close()
        print('Vystupni soubor aleph_data_loans_' + batchFilePostfix + '.tsv byl vytvoren')
        batchFileNo = (i // argBatchSize) + 1
        batchFilePostfix = str(batchFileNo).zfill(4)
        print('Vytvarim vystupni soubor se sekvencnim cislem ' + batchFilePostfix)
        loanstsv = open('iterations/' + iterationDir + '/source_data/loans/' + 'aleph_data_loans_' + batchFilePostfix + '.tsv', 'w', newline='', encoding="utf-8")
        csvwriter = csv.writer(loanstsv, delimiter="\t")
        # zapis hlavicky do vystupu
        write_headers(out_loans_cols, csvwriter)

csvfile.close()
loanstsv.close()
print('Vystupni soubor aleph_data_loans_' + batchFilePostfix + '.tsv byl vytvoren')
print('Hotovo')
