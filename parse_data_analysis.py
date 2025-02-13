import sqlite3
import csv
import urllib.request
import zipfile

gmrs_URL = "https://data.fcc.gov/download/pub/uls/complete/l_gmrs.zip"
gmrs_ZIP = "./data/l_gmrs.zip"
amat_URL = "https://data.fcc.gov/download/pub/uls/complete/l_amat.zip"
amat_ZIP = "./data/l_amat.zip"

gmrs_dir = "./data/l_gmrs/" 
gmrs_HD = "./data/l_gmrs/HD.dat"
gmrs_EN = "./data/l_gmrs/EN.dat"

amat_dir = "./data/l_amat/"
amat_HD = "./data/l_amat/HD.dat"
amat_EN = "./data/l_amat/EN.dat"
amat_AM = "./data/l_amat/AM.dat"

DB_filepath = "callsign.sqlite3"

street_replacement = {
    "." : "",
    "," : "",
    ";" : "",
    "-" : "",
    " Alley" : " Aly",
    " Avenue" : " Ave",
    " Boulevard" : " Blvd",
    " Causeway" : " Cswy",
    " Center" : " Ctr",
    " Circle" : " Cir",
    " Court" : " Ct",
    " Cove" : " Cv",
    " Crossing" : " Xing",
    " Drive" : " Dr",
    " Expressway" : " Expy",
    " Extension" : " Ext",
    " Freeway" : " Fwy",
    " Grove" : " Grv",
    " Highway" : " Hwy",
    " Hollow" : " Holw",
    " Junction" : " Jct",
    " Lane" : " Ln",
    " Motorway" : " Mtwy",
    " Overpass" : " Opas",
    " Parkway" : " Pkwy",
    " Place" : " Pl",
    " Plaza" : " Plz",
    " Point" : " Pt",
    " Road" : " Rd",
    " Route" : " Rte",
    " Skyway" : " Skwy",
    " Square" : " Sq",
    " Street" : " St",
    " Terrace" : " Ter",
    " Trail" : " Trl",
    " " : ""
}

print("Downloading GMRS ZIP from FCC")
urllib.request.urlretrieve(gmrs_URL, gmrs_ZIP)
print("Downloading amateur ZIP from FCC")
urllib.request.urlretrieve(amat_URL, amat_ZIP)

print("Extracting GMRS files from ZIP")
with zipfile.ZipFile(gmrs_ZIP, 'r') as zip_ref:
    zip_ref.extract("HD.dat", gmrs_dir)
    zip_ref.extract("EN.dat", gmrs_dir)

print("Extracting amateur files from ZIP")
with zipfile.ZipFile(amat_ZIP, 'r') as zip_ref:
   zip_ref.extract("HD.dat", amat_dir)
   zip_ref.extract("EN.dat", amat_dir)
   zip_ref.extract("AM.dat", amat_dir)

print("Creating SQLite DB in memory")
con = sqlite3.connect(':memory:')
cur = con.cursor()
cur.execute("CREATE TABLE hd (usid, callsign, status, service)")
cur.execute("CREATE TABLE en (usid, name, street, street_norm, city, state, zip, frn, appl_type)")
cur.execute("CREATE TABLE am (usid, class, prevcall)")

csv.register_dialect('piper', delimiter='|', quoting=csv.QUOTE_NONE)

print("Importing GMRS")
print("  Processing HD")
with open(gmrs_HD, "r") as csvfile:
    good_lines = 0
    bad_lines = 0
    for row in csv.reader(csvfile, dialect='piper'):
        if len(row) == 59 and row[0] == "HD" and row[5] == 'A':
            cur.execute("INSERT INTO hd VALUES (?, ?, ?, ?)", (row[1], row[4], row[5], row[6]))
            good_lines += 1
        else:
            bad_lines += 1
print("    " + str(good_lines) + " / " + str(bad_lines))
            
print("  Bulk EN")
with open(gmrs_EN, "r") as csvfile:
    good_lines = 0
    bad_lines = 0
    for row in csv.reader(csvfile, dialect='piper'):
        if len(row) == 30 and row[0] == "EN":
            street = row[15]
            if(street == '' or street == ' '):
                street = row[19]
            street = street.title()
            for full, abbr in street_replacement.items():
                street_norm = street.replace(full, abbr)
            cur.execute("INSERT INTO en VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (row[1], row[7], street, street_norm, row[16].title(), row[17].upper(), row[18][:5], row[22], row[23]))
            good_lines += 1
        else:
            bad_lines += 1
print("    " + str(good_lines) + " / " + str(bad_lines))

print("Importing Amateur")
print("  Processing HD")
with open(amat_HD, "r") as csvfile:
    good_lines = 0
    bad_lines = 0
    for row in csv.reader(csvfile, dialect='piper'):
        if len(row) == 59 and row[0] == "HD" and row[5] == 'A':
            cur.execute("INSERT INTO hd VALUES (?, ?, ?, ?)", (row[1], row[4], row[5], row[6]))
            good_lines += 1
        else:
            bad_lines += 1
print("    " + str(good_lines) + " / " + str(bad_lines))

print("  Bulk EN")
with open(amat_EN, "r") as csvfile:
    good_lines = 0
    bad_lines = 0
    for row in csv.reader(csvfile, dialect='piper'):
        if len(row) == 30 and row[0] == "EN":
            street = row[15]
            if(street == '' or street == ' '):
                street = row[19]
            street = street.title()
            for full, abbr in street_replacement.items():
                street_norm = street.replace(full, abbr)
            cur.execute("INSERT INTO en VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (row[1], row[7], street, street_norm, row[16].title(), row[17].upper(), row[18][:5], row[22], row[23]))
            good_lines += 1
        else:
            bad_lines += 1
print("    " + str(good_lines) + " / " + str(bad_lines))

print("  Bulk AM")
with open(amat_AM, "r") as csvfile:
    good_lines = 0
    bad_lines = 0
    for row in csv.reader(csvfile, dialect='piper'):
        if len(row) == 18 and row[0] == "AM":
            cur.execute("INSERT INTO am VALUES (?, ?, ?)", (row[1], row[5], row[15]))
            good_lines += 1
        else:
            bad_lines += 1
print("    " + str(good_lines) + " / " + str(bad_lines))

print("Exporting data from SQLite in memory to", DB_filepath)
con.execute("ATTACH DATABASE '" + DB_filepath + "' AS clean;").fetchall()
print("  Inserting table into", DB_filepath)
con.execute("CREATE TABLE clean.licenses AS SELECT hd.usid,callsign,status,service,name,street,street_norm,city,state,zip,frn,appl_type,class,prevcall FROM hd INNER JOIN en ON hd.usid = en.usid LEFT JOIN am ON hd.usid = am.usid;").fetchall()
con.execute("DELETE FROM clean.licenses WHERE appl_type != 'I';").fetchall()
print("  Cleaning up")
con.execute("COMMIT;").fetchall()
con.execute("DETACH clean;").fetchall()
con.commit()
con.close()

print("Optimizing DB")
print("  Connecting")
con = sqlite3.connect(DB_filepath)

print("  Building indices")
con.execute("CREATE INDEX callsign ON licenses (callsign);").fetchall()
con.execute("CREATE INDEX prevcall ON licenses (prevcall);").fetchall()
con.execute("CREATE INDEX frn ON licenses (frn);").fetchall()
con.execute("CREATE INDEX street_search ON licenses (street_norm,city,state);").fetchall()
con.execute("pragma journal_mode = delete;").fetchall()
con.execute("pragma page_size = 1024;").fetchall()

print("  Vacuum")
con.execute("vacuum;").fetchall()

print("Exporting amateur/GMRS matches to CSV")
csv_file_path = 'amat_gmrs_matches.csv'

# Get hams
print("  Getting hams")
con.row_factory = sqlite3.Row
cur = con.cursor()
cur.execute("select * from licenses where service in ('HA','HV');")
amat_rows = cur.fetchall()
results = []
for row in amat_rows:
    if row['frn'] != '':
        gmrs_rec = con.execute("SELECT * FROM licenses WHERE service = 'ZA' AND frn = ?", (row['frn'],)).fetchall()
    if len(gmrs_rec) > 2:
        print(row['callsign'], "has more than one GMRS record at FRN", row['frn'])
    elif len(gmrs_rec) == 1:
        # Export row to CSV with GMRS callsign and match type = FRN
        result = dict(row)
        result['gmrs_callsign'] = gmrs_rec[0]['callsign']
        result['match_type'] = 'FRN'
        results.append(result)
    else:
        gmrs_rec = con.execute("SELECT * FROM licenses WHERE service = 'ZA' AND street_norm = ? AND city = ? AND state = ?", (row['street_norm'], row['city'], row['state'])).fetchall()
        if len(gmrs_rec) > 2:
            print(row['callsign'], "has more than one GMRS record at address", row['street_norm'], row['city'], row['state'])
        elif len(gmrs_rec) == 1:
            # Export row to CSV with GMRS callsign and match type = Address
            result = dict(row)
            result['gmrs_callsign'] = gmrs_rec[0]['callsign']
            result['match_type'] = 'Address'
            results.append(result)


# Writing CSV file
print("  Writing CSV file")
with open(csv_file_path, 'w', newline='') as csvfile:
    fieldnames = results[0].keys()
    csvwriter = csv.DictWriter(csvfile, fieldnames=fieldnames)
    csvwriter.writeheader()
    csvwriter.writerows(results)


# Write all amateur and GMRS records to CSV
cur = con.cursor()
cur.execute("select * from licenses where service in ('HA','HV');")
amat_all = cur.fetchall()
with open('amat_all.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow([desc[0] for desc in cur.description])
    csvwriter.writerows(amat_all)

cur = con.cursor()
cur.execute("select * from licenses where service in ('ZA');")
gmrs_all = cur.fetchall()
with open('gmrs_all.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow([desc[0] for desc in cur.description])
    csvwriter.writerows(gmrs_all)


print("Cleaning up")
con.commit()
con.close()

print("Done")
