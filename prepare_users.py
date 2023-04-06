# interni moduly
import sys, os, argparse
import csv, dbm, json

# konfiguracni parametry
import config

# cislo v nazvu podadresare v adresari iterations
iteration = 1
iterationDir = 'run_' + str(iteration).zfill(3)

# seznam sloupcu, ktere se maji zapsat do vystupniho tsv
export_fields = [
    'Z303_REC_KEY',
    'Z303_TITLE',
    'Z303_HOME_LIBRARY',
    'Z303_FIELD_1',
    'Z303_FIELD_2',
    'Z303_FIELD_3',
    'Z303_NOTE_1',
    'Z303_NOTE_2',
    'Z303_BIRTH_DATE',
    'Z303_GENDER',
    'Z303_BIRTHPLACE',
    'Z303_LAST_NAME',
    'Z303_FIRST_NAME'
]
export_fields += [
    'Z304_ADDRESS1',
    'Z304_ADDRESS2',
    'Z304_ADDRESS3',
    'Z304_ZIP',
    'Z304_EMAIL_ADDRESS',
    'Z304_TELEPHONE',
    'Z304_DATE_FROM',
    'Z304_DATE_TO',
    'Z304_ADDRESS_TYPE',
    'Z304_TELEPHONE_2',
    'Z304_TELEPHONE_3',
    'Z304_TELEPHONE_4',
    'Z304_SMS_NUMBER',
    'Z304_ADDRESS1_02',
    'Z304_ADDRESS2_02',
    'Z304_ADDRESS3_02',
    'Z304_ZIP_02'
]
export_fields += [
    'Z305_BOR_TYPE',
    'Z305_BOR_STATUS',
    'Z305_REGISTRATION_DATE',
    'Z305_EXPIRY_DATE',
    'Z305_NOTE'
]
export_fields += [
    'Z308_BARCODE',
    'Z308_CARDNO'
]


# funkce nacti soubor csv
def csv_open(what, in_file, my_array, my_idx1, my_idx2):
    with open('iterations/' + iterationDir + '/source_data/users/' + in_file, newline='') as csvfile:
        csv_in = csv.reader(csvfile, delimiter=";", quotechar='"')
        for row in csv_in:
            my_array.append(row)
            terms = (row[0] + ' .').split()
            if my_idx2 == None:
                my_idx1.append(row[0])
            elif what == 'z304':
                my_idx1.append(terms[0])
                my_idx2.append(terms[0] + '.' + terms[1])
            elif what == 'z308':
                my_idx1.append(row[3] + '.' + terms[0][0:2])
                my_idx2.append(terms[0][2:])
            else:
                my_idx1.append(terms[0])
                my_idx2.append(terms[1])

        #print(my_array)
        #print('-----')
        #print(my_idx1)
        #print('-----')
        #print(my_idx2)
        #print('-----')
        print('Input file ' + in_file + ' loaded')

    csvfile.close()

# funkce vygeneruje mapu sloupcu pro vystup
def gen_map(my_array, my_isout, ex_fields):
    colnames = my_array[0]
    blankrow = []
    for i in range(len(colnames)):
        if colnames[i] in ex_fields:
            my_isout.append(True)
        else:
            my_isout.append(False)
        blankrow.append('')

    # pridat na konec pole radek prazdnych hodnot
    my_array.append(blankrow)
    #print(my_isout)
    #print('-----')
    print('Column map generated')



# main program

# list pro ulozeni souboru aleph_data_example_z303.csv do pameti
z303 = []
# list pro index nad 1. sloupcem v souboru aleph_data_example_z303.csv
z303_idx1 = []
# mapa oznaceni sloupcu pro vystup, True/False
z303_isout = []
# nacti soubor z303
csv_open('z303', config.fn_user_z303, z303, z303_idx1, None)

# list pro ulozeni souboru aleph_data_example_z304.csv do pameti
z304 = []
z304_idx1 = []
z304_idx2 = []
z304_isout = []
csv_open('z304', config.fn_user_z304, z304, z304_idx1, z304_idx2)

# list pro ulozeni souboru aleph_data_example_z305.csv do pameti
z305 = []
z305_idx1 = []
z305_idx2 = []
z305_isout = []
csv_open('z305', config.fn_user_z305, z305, z305_idx1, z305_idx2)

# list pro ulozeni souboru aleph_data_example_z308.csv do pameti
z308 = []
z308_idx1 = []
z308_idx2 = []
csv_open('z308', config.fn_user_z308, z308, z308_idx1, z308_idx2)

# vygenerovat mapu sloupcu pro vystup v polich z303, z304, z305
gen_map(z303, z303_isout, export_fields)
gen_map(z304, z304_isout, export_fields)
gen_map(z305, z305_isout, export_fields)

# soubor pro ulozeni vysledku ve formatu tsv
userstsv = open('iterations/' + iterationDir + '/source_data/users/' + 'aleph_data_users.tsv', 'w', newline='', encoding="utf-8")
csvwriter = csv.writer(userstsv, delimiter="\t")

# pomocna key-value db pro mapovani user_id -> barcode
userid_barcode_db = dbm.open(config.fn_userid_barcode_db, 'c')

# prochazet vstupni pole z303 a zapisovat vybrane sloupce do vystupu
for i in range(len(z303) - 1):
    row303 = z303[i]
    outline = []
    # cyklus pres vsechny sloupce jednoho radku z303
    for j in range(len(row303)):
        if z303_isout[j]:
            # zapis hodnoty do vystupniho radku
            outline.append(row303[j])
    print(str(i+1) + '. row from z303 processed ...')

    # vyhledat a pripojit adresy z pole z304
    personid = row303[0]
    # nejdrive najit radek s trvalou adresou
    if i == 0:
        # radek s hlavickama sloupcu
        pidindex = 0
    elif personid in z304_idx1:
        # radek s trvalou adresou
        pidindex = z304_idx1.index(personid)
    else:
        # radek prazdnych hodnot
        pidindex = len(z304) - 1

    title304 = z304[0]
    row304 = z304[pidindex]
    # cyklus pres vsechny sloupce jednoho radku z304
    for j in range(len(row304)):
        if z304_isout[j]:
            # zapis hodnoty do vystupniho radku
            outline.append(row304[j])

    # pokracovat hledanim, zda existuje radek s korespondencni adresou
    personid2 = personid + '.02'
    if i == 0:
        # pridat do hlavicky nove sloupce pro korespondencni adresu
        outline.append('Z304_ADDRESS1_02')
        outline.append('Z304_ADDRESS2_02')
        outline.append('Z304_ADDRESS3_02')
        outline.append('Z304_ZIP_02')
    elif personid2 in z304_idx2:
        # nasel se, pridat polozky adresy do novych sloupcu
        pidindex = z304_idx2.index(personid2)
        row304 = z304[pidindex]
        outline.append(row304[title304.index('Z304_ADDRESS1')])
        outline.append(row304[title304.index('Z304_ADDRESS2')])
        outline.append(row304[title304.index('Z304_ADDRESS3')])
        outline.append(row304[title304.index('Z304_ZIP')])
    else:
        # nenasel se, do novych sloupcu dat prazdne hodnoty
        for k in range(4): outline.append('')
    print('Data from z304 appended')
    
    # vyhledat a pripojit udaje z pole z305
    if i == 0:
        pidindex = 0
    elif personid in z305_idx1:
        pidindex = z305_idx1.index(personid)
    else:
        pidindex = len(z305) - 1

    title305 = z305[0]
    row305 = z305[pidindex]
    # cyklus pres vsechny sloupce jednoho radku z305
    for j in range(len(row305)):
        if z305_isout[j]:
            # zapis hodnoty do vystupniho radku
            outline.append(row305[j])
    print('Data from z305 appended')

    # vyhledat a pripojit identifikacni udaje z pole z308
    title308 = z308[0]
    personid01 = personid + '.01'
    personid02 = personid + '.02'
    if i == 0:
        # pridat do hlavicky nove sloupce pro identifikatory
        outline.append('Z308_BARCODE')
        outline.append('Z308_CARDNO')
    else:
        if personid01 in z308_idx1:
            # nasel se barcode, pridat jeho hodnotu do noveho sloupce
            pidindex = z308_idx1.index(personid01)
            ubarcode = z308_idx2[pidindex]
            outline.append(ubarcode)
            # zapsat mapovani user_id -> barcode do pomocne db
            userid_barcode_db[personid] = ubarcode
        else:
            # nenasel se, pridat prazdnou hodnotu
            outline.append('')
            userid_barcode_db[personid] = ''
        if personid02 in z308_idx1:
            # nasel se kod karty, pridat jeho hodnotu do noveho sloupce
            pidindex = z308_idx1.index(personid02)
            outline.append(z308_idx2[pidindex])
        else:
            # nenasel se, pridat prazdnou hodnotu
            outline.append('')
    print('Data from z308 appended')

    # zapis radku do vystupu
    #print('outline = ' + str(outline))
    csvwriter.writerow(outline)

userstsv.close()
print('Output file aleph_data_users.tsv written')
print('Done')
