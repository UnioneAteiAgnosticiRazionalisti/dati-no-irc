#!/usr/bin/env python3

import csv
import sys
import sqlite3
import numpy as np

con = sqlite3.connect("aa_irc2024.db")
con.row_factory = sqlite3.Row

def prepare_db():
    print("Creazione database...")
    cur = con.cursor()
    cur.execute("CREATE TABLE STUDENTI (ANNOSCOLASTICO NUMERIC, CODICESCUOLA TEXT, NUMEROSTUDENTI NUMERIC, STUDENTIIRC NUMERIC, DATODUBBIO TEXT)")
    cur.execute("CREATE TABLE SCUOLE (ANNOSCOLASTICO NUMERIC, AREAGEOGRAFICA TEXT, REGIONE TEXT, PROVINCIA TEXT, CODICESCUOLA TEXT, DENOMINAZIONESCUOLA TEXT, INDIRIZZOSCUOLA TEXT, CAPSCUOLA TEXT, CODICECOMUNESCUOLA TEXT, DESCRIZIONECOMUNE TEXT,  DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA TEXT, INDIRIZZOEMAILSCUOLA TEXT, INDIRIZZOPECSCUOLA TEXT, SITOWEBSCUOLA TEXT, TIPO TEXT)");
    cur.execute("CREATE VIEW DATIRIASSUNTIVI AS SELECT STUDENTI.ANNOSCOLASTICO, STUDENTI.CODICESCUOLA, NUMEROSTUDENTI, STUDENTIIRC, NUMEROSTUDENTI - STUDENTIIRC AS STUDENTINONAVV, AREAGEOGRAFICA, REGIONE, PROVINCIA, CAPSCUOLA, CODICECOMUNESCUOLA, DESCRIZIONECOMUNE, DENOMINAZIONESCUOLA, DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA, TIPO, DATODUBBIO FROM STUDENTI LEFT JOIN SCUOLE ON STUDENTI.CODICESCUOLA = SCUOLE.CODICESCUOLA AND STUDENTI.ANNOSCOLASTICO = SCUOLE.ANNOSCOLASTICO")
    cur.execute("CREATE VIEW RAPPORTODATIMANCANTI AS SELECT 'SÌ' AS 'RIF', ANNOSCOLASTICO, COUNT(*) FROM DATIRIASSUNTIVI WHERE AREAGEOGRAFICA IS NOT NULL GROUP BY ANNOSCOLASTICO UNION SELECT 'NO' AS 'RIF', ANNOSCOLASTICO, COUNT(*) FROM DATIRIASSUNTIVI WHERE AREAGEOGRAFICA IS NULL GROUP BY ANNOSCOLASTICO UNION SELECT 'TOTALE' AS 'RIF', ANNOSCOLASTICO, COUNT(*) FROM DATIRIASSUNTIVI GROUP BY ANNOSCOLASTICO ORDER BY ANNOSCOLASTICO")
    cur.execute("CREATE TABLE LIBRITESTO (CODICESCUOLA TEXT, ANNOCORSO NUMERIC, SEZIONEANNO TEXT, TIPOGRADOSCUOLA TEXT, COMBINAZIONE TEXT, DISCIPLINA TEXT, CODICEISBN TEXT, AUTORI TEXT, TITOLO TEXT, SOTTOTITOLO TEXT, VOLUME TEXT, EDITORE TEXT, PREZZO TEXT, NUOVAADOZ TEXT, DAACQUIST TEXT, CONSIGLIATO TEXT)")
    con.commit()

def load_students(year, source):
    print(f"Caricamento dati studenti da {source} (anno {year})")
    data = []
    cur = con.cursor()
    with open(source) as csv_input:
        csv_reader = csv.DictReader(csv_input, delimiter=',')
        line_count = 0
        skip_count = 0
        skipped_students = 0
        students = 0
        for row in csv_reader:
            skip = None
            #print(row)
            if (row['NUMEROSTUDENTI']=='<=3'):
                skip_count+=1
                skip='NUMEROSTUDENTI<=3'
                row['NUMEROSTUDENTI']=3
            elif (row['STUDENTIIRC']=='<=3'):
                #row['STUDENTIIRC']=3
                skip_count+=1
                skipped_students +=int(row['NUMEROSTUDENTI'])
                skip='STUDENTIIRC<=3'
                row['STUDENTIIRC']=3
            '''
            if (skip):
                continue
            '''
            row['ANNOSCOLASTICO'] = year
            row['DATODUBBIO'] = skip
            data.append(tuple(row.values()))
            line_count += 1
            students += int(row['NUMEROSTUDENTI'])
    cur.executemany("INSERT INTO STUDENTI(CODICESCUOLA, NUMEROSTUDENTI, STUDENTIIRC, ANNOSCOLASTICO, DATODUBBIO) VALUES(" + "?, "*4 + "?)", data)
    con.commit()
    print(f"{line_count} righe caricate, {skip_count} righe saltate")
    print(f"{students} studenti presi in carico, {skipped_students} esclusi per incoerenza dati")

def load_schools(source, kind):
    print(f"Caricamento dati scuole da {source}")
    data = []
    cur = con.cursor()
    with open(source) as csv_input:
        csv_reader = csv.DictReader(csv_input, delimiter=',')
        line_count = 0
        for row in csv_reader:
            #print(row)
            row.pop('SEDESCOLASTICA', None) # solo nei file di TrentinoAA e Valle d'Aosta è presente
            row.pop('CODICEISTITUTORIFERIMENTO', None)
            row.pop('DENOMINAZIONEISTITUTORIFERIMENTO', None)
            row.pop('DESCRIZIONECARATTERISTICASCUOLA', None)
            row.pop('INDICAZIONESEDEDIRETTIVO', None)
            row.pop('INDICAZIONESEDEOMNICOMPRENSIVO', None)
            row['TIPO'] = kind
            data.append(tuple(row.values()))
            line_count += 1
    cur.executemany("INSERT INTO SCUOLE VALUES(" + "?, "*14 +"?)", data)
    con.commit()
    print(str(line_count) + " linee caricate")

def load_books(source):
    print(f"Caricamento dati adozione libri da {source}")
    data = []
    cur = con.cursor()
    with open(source) as csv_input:
        csv_reader = csv.DictReader(csv_input, delimiter=',')
        line_count = 0
        for row in csv_reader:
            #print(row)
            data.append(tuple(row.values()))
            line_count += 1
    cur.executemany("INSERT INTO LIBRITESTO VALUES(" + "?, "*15 +"?)", data)
    con.commit()
    print(str(line_count) + " linee caricate")

def find_anomalies(threshold):
    print("Ricerca di anomalie...")
    cur = con.cursor()
    
    count = 0
    
    totalNumberOfStudents  = 0
    totalNumberOfSchools = 0
    flaggedData = 0
    flaggedSchools = 0
    
    cur.execute("SELECT DISTINCT CODICESCUOLA, DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA FROM SCUOLE WHERE ANNOSCOLASTICO IN (202324) ORDER BY CODICESCUOLA;")

    while True:
        data = cur.fetchone()
        if data == None:
            break
        schoolcode = data['CODICESCUOLA']
        schooltype = data['DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA']
        schoolyears = 5
        if ('primo grado' in schooltype or 'infanzia' in schooltype):
            schoolyears = 3
        count += 1
        #print("--------")
        print(f"{schoolcode} - {schooltype}, {schoolyears}")
        subcur = con.cursor()
        subcur.execute("SELECT * FROM STUDENTI WHERE STUDENTI.CODICESCUOLA = ? AND DATODUBBIO IS NULL ORDER BY ANNOSCOLASTICO;", (schoolcode, ))
        rows = subcur.fetchall()
        if (len(rows) == 0):
            continue

        percentages = []
        students = []
        rstudents = []
        years = []

        flagged = False
        for row in rows:
            for key in row.keys():
                #print(f"{key}: {row[key]}")
                pass
            if int(row['NUMEROSTUDENTI'])>0:
                percentages.append(row['STUDENTIIRC']/row['NUMEROSTUDENTI'])
                years.append(row['ANNOSCOLASTICO'])
                students.append(row['NUMEROSTUDENTI'])
                rstudents.append(row['STUDENTIIRC'])
                #print("----")

        print(percentages)  # TBEE042007 
        print(years)
        print(students)
        print(rstudents)
        percentages = [v for v in percentages if v != 0] # we must get rid of 0 that cause errors in divisions
        for i in range(0, len(percentages)-1):
            #variation = (percentages[i+1]-percentages[i])/percentages[i]
            variation = max(abs((percentages[i+1]-percentages[i])/percentages[i]), abs((percentages[i]-percentages[i+1])/percentages[i+1]))
            if (variation > threshold):
                # 1/schoolyears
                # print(f"({percentages[i+1]}-{percentages[i]})/{percentages[i]}")
                # print(f"{rstudents[i+1]}/{students[i+1]}")
                con.cursor().execute("UPDATE STUDENTI SET DATODUBBIO = 'INCOERENZE TEMPORALI' WHERE CODICESCUOLA = ? AND ANNOSCOLASTICO = ? ", (schoolcode, years[i+1]))
                print(f"Flagged: {schoolcode} - {schooltype}, {schoolyears}")
                flaggedData += 1
                flagged = True

        totalNumberOfSchools +=1
        if flagged:
            flaggedSchools +=1
            print(f"Flagged: {schoolcode} - {schooltype}, {schoolyears}")
            '''
            if (len(percentages)>0):
                print(percentages, end="  --- ")
                arr = np.array(percentages)
                avg = np.sum(arr) / len(percentages)
                print(avg)
            '''
            report = []
            for i in range(0, len(students)):
                report.append(f"{years[i]}: {rstudents[i]}/{students[i]}")
            print("\n".join(report))
            
            con.cursor().execute("UPDATE STUDENTI SET DATODUBBIO = 'INCOERENZE TEMPORALI' WHERE CODICESCUOLA = ?", (schoolcode,))
    
    print(f"Schools: {totalNumberOfSchools}")
    print(f"Flagged: {flaggedSchools}")
    
    #cur.execute("INSERT INTO VALORIMEDIPERSCUOLA (CODICESCUOLA, STUDENTIIRC) SELECT CODICESCUOLA, AVG(STUDENTIIRC) AS STUDENTIIRC FROM STUDENTI GROUP BY CODICESCUOLA")
    con.commit()

def find_anomalies_new(schoolyear, previous_schoolyear):
    print("Ricerca di anomalie...")
    cur = con.cursor()
    
    count = 0
    
    totalNumberOfStudents  = 0
    totalNumberOfSchools = 0
    flaggedData = 0
    flaggedSchools = 0
    
    cur.execute("SELECT DISTINCT CODICESCUOLA, DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA, PROVINCIA, DESCRIZIONECOMUNE FROM SCUOLE WHERE ANNOSCOLASTICO = ?  ORDER BY CODICESCUOLA;", (schoolyear,))

    while True:
        data = cur.fetchone()
        if data == None:
            break
        schoolcode = data['CODICESCUOLA']
        schooltype = data['DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA']
        # print((data['CODICESCUOLA'], data['DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA']))

        subcur = con.cursor()
        subcur.execute("SELECT * FROM STUDENTI WHERE STUDENTI.CODICESCUOLA = ? AND DATODUBBIO IS NULL AND ANNOSCOLASTICO = ?;", (schoolcode, schoolyear))
        info = subcur.fetchone()
        if info == None:
            continue
        #print(dict(info))
        
        # {'ANNOSCOLASTICO': 202324, 'CODICESCUOLA': 'AGPS02000P', 'NUMEROSTUDENTI': 1081, 'STUDENTIIRC': 1068, 'DATODUBBIO': None}
        
        not_irc_students_perc = _compute_percentage(info)
        #if not_irc_students_perc > .5:
        #    print(f"* {not_irc_students_perc}")
        #print(f"* {not_irc_students_perc}")
        
        if not_irc_students_perc > .5 and (data['DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA']=='SCUOLA PRIMARIA' or data['DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA']=='SCUOLA INFANZIA'):
            subsubcur = con.cursor()
            subsubcur.execute("SELECT * FROM STUDENTI WHERE STUDENTI.CODICESCUOLA = ? AND DATODUBBIO IS NULL AND ANNOSCOLASTICO = ?;", (schoolcode, previous_schoolyear))
            info2 = subsubcur.fetchone()
            previous_year_perc = -1
            report = [data['CODICESCUOLA'], data['DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA'], data['PROVINCIA'], data['DESCRIZIONECOMUNE'], not_irc_students_perc, info['NUMEROSTUDENTI'], info['NUMEROSTUDENTI'] - info['STUDENTIIRC']]
            if info2 != None:
                previous_year_perc = _compute_percentage(info2)
                report.append(previous_year_perc)
                report.append(info2['NUMEROSTUDENTI'])
                report.append(info2['NUMEROSTUDENTI'] - info2['STUDENTIIRC'])
            print(report)
            

def _compute_percentage(info):
    students = info['NUMEROSTUDENTI']
    not_irc_students = students - info['STUDENTIIRC']
    return 1.0 * not_irc_students / students

def remove_subjects():
    print("Eliminazione delle materie curricolari ordinarie...")
    cur = con.cursor()
    con.execute("DELETE FROM LIBRITESTO WHERE DISCIPLINA NOT IN ('ADOZIONE ALTERNATIVA ART. 156 D.L. 297/94', 'IRC O ATTIVITA ALTERNATIVE');")
    con.commit()
    cur.execute('VACUUM;')
    con.commit()

if __name__ == '__main__':
    prepare_db()
    load_students(201819, 'DATIPRECEDENTI201819.csv')
    load_students(201920, 'DATIPRECEDENTI201920.csv')
    load_students(202021, 'DATIPRECEDENTI202021.csv')
    load_students(202122, 'dati_scuole_20212022.csv')
    load_students(202223, 'dati_scuole_20222023.csv')
    load_students(202324, 'dati_scuole_20232024.csv')
    
    load_students(201819, 'bolzano_ita201819.csv')
    load_students(201920, 'bolzano_ita201920.csv')
    load_students(202021, 'bolzano_ita202021.csv')
    load_students(202122, 'bolzano_ita202122.csv')
    load_students(202223, 'bolzano_ita202223.csv')
    load_students(202324, 'bolzano_ita202324.csv')

    load_students(201920, 'bolzano_ted201920.csv')
    load_students(202021, 'bolzano_ted202021.csv')
    load_students(202122, 'bolzano_ted202122.csv')
    load_students(202223, 'bolzano_ted202223.csv')
    load_students(202324, 'bolzano_ted202324.csv')

    load_students(201819, 'bolzano_lad201819.csv')
    load_students(201920, 'bolzano_lad201920.csv')
    load_students(202021, 'bolzano_lad202021.csv')
    load_students(202122, 'bolzano_lad202122.csv')
    load_students(202223, 'bolzano_lad202223.csv')
    load_students(202324, 'bolzano_lad202324.csv')
    
    load_students(201819, 'trento201819.csv')
    load_students(201920, 'trento201920.csv')
    load_students(202021, 'trento202021.csv')
    load_students(202122, 'trento202122.csv')
    load_students(202223, 'trento202223.csv')
    load_students(202324, 'trento202324.csv')
    
    for source in [
        #'SCUANAAUTSTAT20212220210901.csv', # pubbliche delle regioni autonome, 2021/22
        #'SCUANAAUTSTAT20222320220901.csv', # pubbliche delle regioni autonome, 2022/23
        'SCUANAAUTSTAT20232420230901.csv', # pubbliche delle regioni autonome, 2023/24
        #'SCUANAGRAFESTAT20212220220831.csv', # pubbliche statali, 2021/22
        #'SCUANAGRAFESTAT20222320220901.csv', # pubbliche statali, 2022/23'''
        'SCUANAGRAFESTAT20232420230901.csv', # pubbliche statali, 2023/24
        #'INTEGRAZIONEALTOADIGE202223.csv',
        #'INTEGRAZIONEALTOADIGE202324.csv'
        ]:
            load_schools(source, 'PUBBLICA')

    for source in [
        #'SCUANAAUTPAR20212220210901.csv', # private delle regioni autonome, 2021/22
        #'SCUANAAUTPAR20222320220901.csv', # private delle regioni autonome, 2022/23
        'SCUANAAUTPAR20232420230901.csv', # private delle regioni autonome, 2023/24
        #'SCUANAGRAFEPAR20212220220831.csv', # private nazionali, 2021/22
        #'SCUANAGRAFEPAR20222320220901.csv', # private nazionali, 2022/23
        'SCUANAGRAFEPAR20232420230901.csv',  # private nazionali, 2023/24
        ]:
            load_schools(source, 'PARITARIA')

    '''
    for source in [
        'ALTABRUZZO000020230913.csv',
        'ALTBASILICATA000020230913.csv',
        'ALTCALABRIA000020230913.csv',
        'ALTEMILIAROMAGNA000020230913.csv',
        'ALTFRIULIVENEZIAGIULIA000020230913.csv',
        'ALTLAZIO000020230913.csv',
        'ALTLIGURIA000020230913.csv',
        'ALTLOMBARDIA000020230913.csv',
        'ALTMARCHE000020230913.csv',
        'ALTMOLISE000020230913.csv',
        'ALTPIEMONTE000020230913.csv',
        'ALTPUGLIA000020230913.csv',
        'ALTSARDEGNA000020230913.csv',
        'ALTSICILIA000020230913.csv',
        'ALTTOSCANA000020230913.csv',
        'ALTTRENTINOALTOADIGE000020230913.csv',
        'ALTUMBRIA000020230913.csv',
        'ALTVALLEDAOSTA000020230913.csv',
        'ALTVENETO000020230913.csv',
        ]:
            load_books(source)
            pass
    '''
    #remove_subjects();
    #find_anomalies(202324, 202223)
    find_anomalies(.75)
