#!/bin/bash

update() {
    echo "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Infanzia' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE '%INFANZIA%';" | sqlite3 aa_irc2023.db 

    echo "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Primaria' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE '%PRIMARIA%';" | sqlite3 aa_irc2023.db 

    echo "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Secondaria primo grado' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE '%PRIMO GRADO%';" | sqlite3 aa_irc2023.db 

    echo "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Istituto professionale' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE '%PROF%';" | sqlite3 aa_irc2023.db 

    echo "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Istituto tecnico' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE 'IST TEC%' OR DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE 'ISTITUTO TECNICO%';" | sqlite3 aa_irc2023.db 

    echo "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Liceo' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE 'LICEO%' OR DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA IN ('ISTITUTO D''ARTE', 'SCUOLA MAGISTRALE', 'ISTITUTO MAGISTRALE', 'ISTITUTO SUPERIORE');" | sqlite3 aa_irc2023.db 

    echo "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Altra scuola secondaria paritaria' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE 'SCUOLA SEC.%';" | sqlite3 aa_irc2023.db 

    echo "UPDATE SCUOLE SET DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA = 'Tipo di scuola non disponibile' WHERE DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA LIKE '%COMPR%';" | sqlite3 aa_irc2023.db 
    
}

show_by_region() {
    ANNO=$1
    echo "Non avvalentisi per regione, anno $ANNO, scuole pubbliche"

    echo "SELECT ANNOSCOLASTICO, REGIONE, SUM(NUMEROSTUDENTI), SUM(STUDENTINONAVV), ROUND(SUM(STUDENTINONAVV)*100.0/SUM(NUMEROSTUDENTI), 2) AS PERCNONAVV FROM DATIRIASSUNTIVI WHERE AREAGEOGRAFICA IS NOT NULL AND TIPO = 'PUBBLICA' AND ANNOSCOLASTICO = $ANNO AND DATODUBBIO IS NULL GROUP BY ANNOSCOLASTICO, REGIONE ORDER BY PERCNONAVV DESC" | sqlite3 -column -header aa_irc2023.db
}

show_by_district() {
    ANNO=$1
    echo "Non avvalentisi per provincia, anno $ANNO, scuole pubbliche"

    echo "SELECT ANNOSCOLASTICO, PROVINCIA, SUM(NUMEROSTUDENTI), SUM(STUDENTINONAVV), ROUND(SUM(STUDENTINONAVV)*100.0/SUM(NUMEROSTUDENTI), 2) AS PERCNONAVV FROM DATIRIASSUNTIVI WHERE AREAGEOGRAFICA IS NOT NULL AND TIPO = 'PUBBLICA' AND ANNOSCOLASTICO = $ANNO AND DATODUBBIO IS NULL GROUP BY ANNOSCOLASTICO, PROVINCIA ORDER BY PERCNONAVV DESC" | sqlite3 -column -header aa_irc2023.db
}


show_by_schoolkind() {
    ANNO=$1
    echo "Non avvalentisi per tipologia di scuola, anno $ANNO, scuole pubbliche"
    echo "SELECT ANNOSCOLASTICO, DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA AS TIPOSCUOLA, SUM(NUMEROSTUDENTI), SUM(STUDENTINONAVV), ROUND(SUM(STUDENTINONAVV)*100.0/SUM(NUMEROSTUDENTI), 2) AS PERCNONAVV FROM DATIRIASSUNTIVI WHERE AREAGEOGRAFICA IS NOT NULL AND TIPO = 'PUBBLICA' AND ANNOSCOLASTICO = $ANNO AND DATODUBBIO IS NULL GROUP BY ANNOSCOLASTICO, DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA ORDER BY PERCNONAVV DESC" | sqlite3 -column -header aa_irc2023.db
}

show_top_ten() {
    ANNO=$1
    echo "Top ten, anno $ANNO, scuole pubbliche"
    echo "SELECT CODICESCUOLA, DENOMINAZIONESCUOLA, DESCRIZIONECOMUNE, DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA AS TIPOSCUOLA, NUMEROSTUDENTI, STUDENTINONAVV, ROUND(STUDENTINONAVV*100.0/NUMEROSTUDENTI, 2) AS PERCNONAVV FROM DATIRIASSUNTIVI WHERE AREAGEOGRAFICA IS NOT NULL AND TIPO = 'PUBBLICA' AND ANNOSCOLASTICO = $ANNO AND DATODUBBIO IS NULL ORDER BY PERCNONAVV DESC LIMIT 10" | sqlite3 -column -header aa_irc2023.db
}

show_top_ten_district() {
    ANNO=$1
    PROVINCIA=$2
    echo "Top ten, anno $ANNO, scuole pubbliche, provincia di $PROVINCIA"
    echo "SELECT CODICESCUOLA, DENOMINAZIONESCUOLA, DESCRIZIONECOMUNE, DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA AS TIPOSCUOLA, NUMEROSTUDENTI, STUDENTINONAVV, ROUND(STUDENTINONAVV*100.0/NUMEROSTUDENTI, 2) AS PERCNONAVV FROM DATIRIASSUNTIVI WHERE AREAGEOGRAFICA IS NOT NULL AND TIPO = 'PUBBLICA' AND ANNOSCOLASTICO = $ANNO AND DATODUBBIO IS NULL AND PROVINCIA ='$PROVINCIA' ORDER BY PERCNONAVV DESC LIMIT 10" | sqlite3 -column -header aa_irc2023.db
}

show_city() {
    ANNO=$1
    COMUNE=$2
    echo "Classifica completa, anno $ANNO, scuole pubbliche, comune di $COMUNE"
    echo "SELECT CODICESCUOLA, DENOMINAZIONESCUOLA, DESCRIZIONECOMUNE, DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA AS TIPOSCUOLA, NUMEROSTUDENTI, STUDENTINONAVV, ROUND(STUDENTINONAVV*100.0/NUMEROSTUDENTI, 2) AS PERCNONAVV FROM DATIRIASSUNTIVI WHERE AREAGEOGRAFICA IS NOT NULL AND TIPO = 'PUBBLICA' AND ANNOSCOLASTICO = $ANNO AND DATODUBBIO IS NULL AND DESCRIZIONECOMUNE ='$COMUNE' ORDER BY PERCNONAVV DESC" | sqlite3 -column -header aa_irc2023.db
}

###############

update

echo "Sintesi generale"
echo "SELECT ANNOSCOLASTICO, SUM(NUMEROSTUDENTI), SUM(STUDENTINONAVV), ROUND(SUM(STUDENTINONAVV)*100.0/SUM(NUMEROSTUDENTI), 2) AS PERCNONAVV FROM DATIRIASSUNTIVI WHERE AREAGEOGRAFICA IS NOT NULL AND TIPO = 'PUBBLICA'  GROUP BY ANNOSCOLASTICO ORDER BY ANNOSCOLASTICO ASC" | sqlite3 -column -header aa_irc2023.db

echo -e "\n\n"
echo "Dati dubbi non tenuti in considerazione"
echo "SELECT ANNOSCOLASTICO, COUNT(CODICESCUOLA), DATODUBBIO FROM DATIRIASSUNTIVI WHERE DATODUBBIO IS NOT NULL GROUP BY ANNOSCOLASTICO, DATODUBBIO" | sqlite3 -column -header aa_irc2023.db


echo -e "\n\n"
echo "Dati tenuti in considerazione"
echo "SELECT ANNOSCOLASTICO, COUNT(CODICESCUOLA) FROM DATIRIASSUNTIVI WHERE DATODUBBIO IS NULL GROUP BY ANNOSCOLASTICO" | sqlite3 -column -header aa_irc2023.db

for YEAR in 202122 202223; do
    echo -e "\n\n"
    show_by_region $YEAR
    echo -e "\n\n"
    show_by_district $YEAR
    echo -e "\n\n"
    show_by_schoolkind $YEAR
    echo -e "\n\n"
    show_top_ten $YEAR
done