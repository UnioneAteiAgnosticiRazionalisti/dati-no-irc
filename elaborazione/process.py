#!/usr/bin/env python3

import csv
import sys
import sqlite3
import numpy as np

con = sqlite3.connect("aa_irc2025.db")
con.row_factory = sqlite3.Row

def prepare_db():
    print("Creazione database...")
    cur = con.cursor()
    cur.execute("CREATE TABLE STUDENTI (ANNOSCOLASTICO NUMERIC, CODICESCUOLA TEXT, NUMEROSTUDENTI NUMERIC, STUDENTIIRC NUMERIC NULL, STUDENTIIRCT TEXT, STUDENTIIRCS NUMERIC, DATODUBBIO TEXT)")
    cur.execute("CREATE TABLE SCUOLE (ANNOSCOLASTICO NUMERIC, AREAGEOGRAFICA TEXT, REGIONE TEXT, PROVINCIA TEXT, CODICESCUOLA TEXT, DENOMINAZIONESCUOLA TEXT, INDIRIZZOSCUOLA TEXT, CAPSCUOLA TEXT, CODICECOMUNESCUOLA TEXT, DESCRIZIONECOMUNE TEXT,  DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA TEXT, INDIRIZZOEMAILSCUOLA TEXT, INDIRIZZOPECSCUOLA TEXT, SITOWEBSCUOLA TEXT, TIPO TEXT)")
    cur.execute("CREATE TABLE COMUNI (ANNOSCOLASTICO NUMERIC, CODICEISTAT NUMERIC UNIQUE, CODICECATASTALE TEXT UNIQUE, NUMEROABITANTI NUMERIC, DENOMINAZIONECOMUNE TEXT)")
    cur.execute("CREATE VIEW DATIRIASSUNTIVI AS SELECT STUDENTI.ANNOSCOLASTICO, STUDENTI.CODICESCUOLA, NUMEROSTUDENTI, STUDENTIIRC, STUDENTIIRCS, STUDENTIIRCT, NUMEROSTUDENTI - STUDENTIIRCS AS STUDENTINONAVV, AREAGEOGRAFICA, REGIONE, PROVINCIA, CAPSCUOLA, CODICECOMUNESCUOLA, DESCRIZIONECOMUNE, DENOMINAZIONESCUOLA, DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA, TIPO, DATODUBBIO, NUMEROABITANTI FROM STUDENTI LEFT JOIN SCUOLE ON STUDENTI.CODICESCUOLA = SCUOLE.CODICESCUOLA AND STUDENTI.ANNOSCOLASTICO = SCUOLE.ANNOSCOLASTICO LEFT JOIN COMUNI ON SCUOLE.CODICECOMUNESCUOLA = COMUNI.CODICECATASTALE AND SCUOLE.ANNOSCOLASTICO = COMUNI.ANNOSCOLASTICO")
    cur.execute("CREATE VIEW RAPPORTODATIMANCANTI AS SELECT 'SÌ' AS 'RIF', ANNOSCOLASTICO, COUNT(*) FROM DATIRIASSUNTIVI WHERE AREAGEOGRAFICA IS NOT NULL GROUP BY ANNOSCOLASTICO UNION SELECT 'NO' AS 'RIF', ANNOSCOLASTICO, COUNT(*) FROM DATIRIASSUNTIVI WHERE AREAGEOGRAFICA IS NULL GROUP BY ANNOSCOLASTICO UNION SELECT 'TOTALE' AS 'RIF', ANNOSCOLASTICO, COUNT(*) FROM DATIRIASSUNTIVI GROUP BY ANNOSCOLASTICO ORDER BY ANNOSCOLASTICO")
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
            #print(row)
            irc_students_text = row['STUDENTIIRC']
            if (row['NUMEROSTUDENTI']=='<=3'):
                row['NUMEROSTUDENTI']=None
            if (row['STUDENTIIRC']=='<=3'):
                row['STUDENTIIRC']=3
            row['STUDENTIIRCT'] = irc_students_text
            row['ANNOSCOLASTICO'] = year
            if (row['STUDENTIIRC']==3): # bisogna rispettare l'ordine di elementi del dizionario
                row['DATODUBBIO'] = 'STUDENTI IRC<=3'
            else:
                row['DATODUBBIO'] = ''
            data.append(tuple(row.values()))
            line_count += 1
    cur.executemany("INSERT INTO STUDENTI(CODICESCUOLA, NUMEROSTUDENTI, STUDENTIIRC, STUDENTIIRCT, ANNOSCOLASTICO, DATODUBBIO) VALUES(" + "?, "*5 + "?)", data)
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

def load_towns(year, source):
    print(f"Caricamento dati comuni da {source} (anno {year})")
    data = []
    cur = con.cursor()
    with open(source) as csv_input:
        csv_reader = csv.DictReader(csv_input, delimiter=',')
        line_count = 0
        for row in csv_reader:
            row['ANNOSCOLASTICO'] = year
            print(row)
            data.append(tuple(row.values()))
            line_count += 1
    cur.executemany("INSERT INTO COMUNI(CODICEISTAT, DENOMINAZIONECOMUNE, NUMEROABITANTI, CODICECATASTALE, ANNOSCOLASTICO) VALUES(" + "?, "*4 + "?)", data)
    
    con.commit()
    print(f"{line_count} righe caricate")

def _compute_percentage(info):
    students = info['NUMEROSTUDENTI']
    not_irc_students = students - info['STUDENTIIRC']
    return 1.0 * not_irc_students / students

def normalize_school_values():
    cur = con.cursor()
    for query in [
        "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Infanzia' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE '%INFANZIA%';",
        "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Primaria' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE '%PRIMARIA%';", 
        "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Secondaria primo grado' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE '%PRIMO GRADO%';",
        "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Istituto professionale' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE '%PROF%';",
        "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Istituto tecnico' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE 'IST TEC%' OR DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE 'ISTITUTO TECNICO%';",
        "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Liceo' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE 'LICEO%' OR DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE 'Liceo%' OR DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA IN ('ISTITUTO D''ARTE', 'SCUOLA MAGISTRALE', 'ISTITUTO MAGISTRALE', 'ISTITUTO SUPERIORE');",
        "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Altra scuola secondaria paritaria' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE 'SCUOLA SEC.%';",
        "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Tipo di scuola non disponibile' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE '%COMPR%';"
    ]:
        print(f"Executing: {query}")
        con.execute(query)
        con.commit()

def vacuum():       
    cur = con.cursor()
    print("Pulizia e compattamento...")
    cur.execute('VACUUM;')
    con.commit()

def impute_missing_student_values(year):
    cur = con.cursor()

    print("Rimozione scuole senza studenti")
    cur.execute("DELETE FROM STUDENTI WHERE NUMEROSTUDENTI IS NULL")

    print("Copia del dato di studenti irc")
    cur.execute("UPDATE STUDENTI SET STUDENTIIRCS = STUDENTIIRC")

    sql_missing = """
    SELECT
        st.ROWID,
        st.CODICESCUOLA,
        st.NUMEROSTUDENTI,
        s.DESCRIZIONECOMUNE,
        s.PROVINCIA,
        s.DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA,
        s.DENOMINAZIONESCUOLA
    FROM STUDENTI st
    JOIN SCUOLE s
      ON st.CODICESCUOLA = s.CODICESCUOLA
     AND st.ANNOSCOLASTICO = s.ANNOSCOLASTICO
    WHERE st.DATODUBBIO = 'STUDENTI IRC<=3'
      AND st.NUMEROSTUDENTI > 0
      AND st.ANNOSCOLASTICO = ?
      AND s.TIPO = 'PUBBLICA'
    """
    cur.execute(sql_missing, (year,))

    missing = cur.fetchall()
    updates = []

    print(f"Trovate {len(missing)} tuple con dati mancanti.\n")
    skipped = 0

    for rowid, codice, n_total, comune, provincia, tipo, name in missing:

        print("="*70)
        print(f"{comune}: {name} ({codice})")
        print(f"Provincia: {provincia}")
        print(f"Tipo: {tipo}")
        print(f"Totale studenti: {n_total}\n")

        sql_sisters = """
        SELECT
            st.NUMEROSTUDENTI,
            st.STUDENTIIRC,
            s.DENOMINAZIONESCUOLA,
            s.DESCRIZIONECOMUNE
        FROM STUDENTI st
        JOIN SCUOLE s
          ON st.CODICESCUOLA = s.CODICESCUOLA
         AND st.ANNOSCOLASTICO = s.ANNOSCOLASTICO
        WHERE st.DATODUBBIO != 'STUDENTI IRC<=3'
          AND s.PROVINCIA = ?
          AND s.DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = ?
          AND st.ANNOSCOLASTICO = ?
          AND s.TIPO = 'PUBBLICA'
        """

        params = [provincia, tipo, year]
        cur.execute(sql_sisters, params)
        sisters = cur.fetchall()

        if not sisters:
            print("⚠ Nessuna scuola comparabile. Lasciato valore nullo.\n")
            skipped+=1
            continue


        total_students = 0
        total_irc = 0

        for n_tot, n_irc, sister_name, sister_town in sisters:
            print(f"- {sister_name} ({sister_town}): IRC={n_irc}, TOT={n_tot}")
            total_students += n_tot
            total_irc += n_irc

        ratio = total_irc / total_students
        estimated = round(n_total * ratio)

        print()
        print(f"Numero totale studenti avvalentisi nella provincia: {total_irc}")
        print(f"Numero totale studenti nella provincia: {total_students}")
        print(f"Percentuale provinciale = {total_irc}/{total_students} = {ratio:.3f}")
        print(f"Valore calcolato per numero avvalentisi = round({n_total} × {ratio:.3f}) = {estimated}\n")

        updates.append((estimated, "STUDENTIIRC<=3: Valore calcolato in base alla media provinciale", rowid))

    print(f"\nAggiornamento di {len(updates)} tuple…")

    cur.executemany("""
    UPDATE STUDENTI
    SET STUDENTIIRCS = ?,
        DATODUBBIO = ?
    WHERE ROWID = ?
    """, updates)

    con.commit()

    print(f"Fatto. Non stimato valore per {skipped} scuole su {len(missing)}.")

def fix_schools():
    # due scuole della Valle d'Aosta sono inserite nell'elenco delle scuole pubbliche, ma sono private
    cur = con.cursor()
    cur.execute("UPDATE SCUOLE SET TIPO='PRIVATA' WHERE CODICESCUOLA IN ('AORI47500D', 'AOPL60500E')")
    con.commit()
    
def copy_irc_values():
    cur = con.cursor()
    cur.execute("UPDATE STUDENTI SET STUDENTIIRCS = STUDENTIIRC")
    con.commit()
    
def find_anomalies_with_previous_year(year, previous_year):
    cur = con.cursor()

    sql_anomalies = """
    SELECT
        st.ROWID,
        st.CODICESCUOLA,
        st.NUMEROSTUDENTI,
        st.STUDENTIIRC,
        s.DESCRIZIONECOMUNE,
        s.PROVINCIA,
        s.DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA,
        s.DENOMINAZIONESCUOLA,
        st.DATODUBBIO
    FROM STUDENTI st
    JOIN SCUOLE s
      ON st.CODICESCUOLA = s.CODICESCUOLA
     AND st.ANNOSCOLASTICO = s.ANNOSCOLASTICO
    WHERE st.DATODUBBIO != 'STUDENTIIRC<=3: Valore calcolato in base alla media provinciale'
      AND st.STUDENTIIRC * 1.0 / st.NUMEROSTUDENTI < 0.20
      AND st.NUMEROSTUDENTI > 0
      AND st.ANNOSCOLASTICO = ?
      AND s.TIPO = 'PUBBLICA'
    """
    cur.execute(sql_anomalies, (year,))

    anomalies = cur.fetchall()
    updates = []

    print(f"Trovate {len(anomalies)} tuple con dati probabilmente errati.\n")
    skipped = 0

    for rowid, codice, n_total, n_irc, comune, provincia, tipo, name, dubbio in anomalies:

        current_ratio = n_irc * 1.0 / n_total
        print("="*70)
        print(f"{comune}: {name} ({codice})")
        print(f"Provincia: {provincia}")
        print(f"Tipo: {tipo}")
        print(f"Totale studenti: {n_total}")
        print(f"Studenti IRC: {n_irc}")
        print(f"Percentuale: {current_ratio}")

        sql_previous_year = """
        SELECT
            NUMEROSTUDENTI,
            STUDENTIIRC
        FROM STUDENTI
        WHERE CODICESCUOLA = ?
          AND ANNOSCOLASTICO = ?
        """

        params = [codice, previous_year]
        cur.execute(sql_previous_year, params)
        previous_year_data = cur.fetchone()

        print("-"*70)

        if not previous_year_data:
            print("Dati scuola non trovati per l'anno precedente.\n")
            #skip+=1
            #continue
            estimated = n_total - n_irc
            print("Assunta inversione valore.\n")
            updates.append((estimated, "PERCENTUALE ANOMALA: Assunta inversione valore", rowid))
        
        else:
            previous_year_ratio = previous_year_data['STUDENTIIRC']*1.0 / previous_year_data['NUMEROSTUDENTI']
            print(f"Totale studenti: {previous_year_data['NUMEROSTUDENTI']}")
            print(f"Studenti IRC: {previous_year_data['STUDENTIIRC']}")
            print(f"Percentuale precedente: {previous_year_ratio}")

            # ratio = total_irc / total_students
            # estimated = round(n_total * ratio)
            
            if (previous_year_ratio - current_ratio > 0.5):
                estimated = round(n_total * previous_year_ratio)
                updates.append((estimated, "PERCENTUALE ANOMALA: Valore calcolato in base al tasso dell'anno precedente", rowid))
                print(f"Valore stimato: {estimated}")
            else:
                print(f"Valore lasciato inalterato")
                skipped+=1

    print(f"\nAggiornamento di {len(updates)} tuple…")

    cur.executemany("""
    UPDATE STUDENTI
    SET STUDENTIIRCS = ?,
        DATODUBBIO = ?
    WHERE ROWID = ?
    """, updates)

    con.commit()

    print(f"Fatto. Non stimato valore per {skipped} scuole su {len(anomalies)}.")

def find_anomalies(year):
    cur = con.cursor()

    sql_anomalies = """
    SELECT
        st.ROWID,
        st.CODICESCUOLA,
        st.NUMEROSTUDENTI,
        st.STUDENTIIRC,
        s.DESCRIZIONECOMUNE,
        s.PROVINCIA,
        s.DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA,
        s.DENOMINAZIONESCUOLA,
        st.DATODUBBIO
    FROM STUDENTI st
    JOIN SCUOLE s
      ON st.CODICESCUOLA = s.CODICESCUOLA
     AND st.ANNOSCOLASTICO = s.ANNOSCOLASTICO
    WHERE st.DATODUBBIO != 'STUDENTIIRC<=3: Valore calcolato in base alla media provinciale'
      AND st.STUDENTIIRC * 1.0 / st.NUMEROSTUDENTI < 0.15
      AND st.NUMEROSTUDENTI > 0
      AND st.ANNOSCOLASTICO = ?
      AND s.TIPO = 'PUBBLICA'
    """
    cur.execute(sql_anomalies, (year,))

    anomalies = cur.fetchall()
    updates = []

    print(f"Trovate {len(anomalies)} tuple con dati probabilmente errati.\n")
    skipped = 0

    for rowid, codice, n_total, n_irc, comune, provincia, tipo, name, dubbio in anomalies:

        current_ratio = n_irc * 1.0 / n_total
        print("="*70)
        print(f"{comune}: {name} ({codice})")
        print(f"Provincia: {provincia}")
        print(f"Tipo: {tipo}")
        print(f"Totale studenti: {n_total}")
        print(f"Studenti IRC: {n_irc}")
        print(f"Percentuale: {current_ratio}")

        estimated = n_total - n_irc
        print("Assunta inversione valore.\n")
        updates.append((estimated, "PERCENTUALE ANOMALA: Assunta inversione valore", rowid))
        
    print(f"\nAggiornamento di {len(updates)} tuple…")

    cur.executemany("""
    UPDATE STUDENTI
    SET STUDENTIIRCS = ?,
        DATODUBBIO = ?
    WHERE ROWID = ?
    """, updates)

    con.commit()

    print(f"Fatto. Non stimato valore per {skipped} scuole su {len(anomalies)}.")


def load_data():
    load_towns(202425, 'comuni.csv')
    
    #load_students(201819, 'DATIPRECEDENTI201819.csv')
    #load_students(201920, 'DATIPRECEDENTI201920.csv')
    #load_students(202021, 'DATIPRECEDENTI202021.csv')
    load_students(202122, 'dati_scuole_20212022.csv')
    load_students(202223, 'dati_scuole_20222023.csv')
    load_students(202324, 'dati_scuole_20232024.csv')
    load_students(202425, 'dati_scuole_20242025.csv')
    
    #load_students(201819, 'bolzano_ita201819.csv')
    #load_students(201920, 'bolzano_ita201920.csv')
    #load_students(202021, 'bolzano_ita202021.csv')
    load_students(202122, 'bolzano_ita202122.csv')
    load_students(202223, 'bolzano_ita202223.csv')
    load_students(202324, 'bolzano_ita202324.csv')
    load_students(202425, 'bolzano_ita202425.csv')

    #load_students(201920, 'bolzano_ted201920.csv')
    #load_students(202021, 'bolzano_ted202021.csv')
    load_students(202122, 'bolzano_ted202122.csv')
    load_students(202223, 'bolzano_ted202223.csv')
    load_students(202324, 'bolzano_ted202324.csv')
    load_students(202425, 'bolzano_ted202425.csv')

    #load_students(201819, 'bolzano_lad201819.csv')
    #load_students(201920, 'bolzano_lad201920.csv')
    #load_students(202021, 'bolzano_lad202021.csv')
    load_students(202122, 'bolzano_lad202122.csv')
    load_students(202223, 'bolzano_lad202223.csv')
    load_students(202324, 'bolzano_lad202324.csv')
    load_students(202425, 'bolzano_lad202425.csv')
    
    #load_students(201819, 'trento201819.csv')
    #load_students(201920, 'trento201920.csv')
    #load_students(202021, 'trento202021.csv')
    load_students(202122, 'trento202122.csv')
    load_students(202223, 'trento202223.csv')
    load_students(202324, 'trento202324.csv')
    load_students(202425, 'trento202425.csv')
    
    for source in [
        'SCUANAAUTSTAT20212220210901.csv', # pubbliche delle regioni autonome, 2021/22
        'SCUANAAUTSTAT20222320220901.csv', # pubbliche delle regioni autonome, 2022/23
        'SCUANAAUTSTAT20232420230901.csv', # pubbliche delle regioni autonome, 2023/24
        'SCUANAAUTSTAT20242520240901.csv', # pubbliche delle regioni autonome, 2024/25
        'SCUANAGRAFESTAT20212220220831.csv', # pubbliche statali, 2021/22
        'SCUANAGRAFESTAT20222320220901.csv', # pubbliche statali, 2022/23'''
        'SCUANAGRAFESTAT20232420230901.csv', # pubbliche statali, 2023/24
        'SCUANAGRAFESTAT20242520240901.csv', # pubbliche statali, 2025/25
        'INTEGRAZIONEALTOADIGE202223.csv',
        'INTEGRAZIONEALTOADIGE202324.csv',
        'INTEGRAZIONETRENTINOALTOADIGE202425.csv'
        ]:
            load_schools(source, 'PUBBLICA')

    for source in [
        'SCUANAAUTPAR20212220210901.csv', # private delle regioni autonome, 2021/22
        'SCUANAAUTPAR20222320220901.csv', # private delle regioni autonome, 2022/23
        'SCUANAAUTPAR20232420230901.csv', # private delle regioni autonome, 2023/24
        'SCUANAAUTPAR20242520240901.csv', # private delle regioni autonome, 2024/25

        'SCUANAGRAFEPAR20212220220831.csv', # private nazionali, 2021/22
        'SCUANAGRAFEPAR20222320220901.csv', # private nazionali, 2022/23
        'SCUANAGRAFEPAR20232420230901.csv',  # private nazionali, 2023/24
        'SCUANAGRAFEPAR20242520240901.csv',  # private nazionali, 2024/25
        ]:
            load_schools(source, 'PARITARIA')
    fix_schools()
    
if __name__ == '__main__':
    prepare_db()
    load_data()
    
    normalize_school_values()
    for year in (202021, 202122, 202223, 202324, 202425):
        impute_missing_student_values(year)
        find_anomalies(year)
        pass
    
    vacuum()
    con.close()
