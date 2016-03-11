import psycopg2
import os


conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
cur = conn.cursor()

for i in range(5,101,5):

    query = "SELECT SUM(CASE WHEN S < " + str(i) + " THEN daysInMonth ELSE 0 END)/SUM(daysInMonth)::float \
    FROM ( SELECT SUM(TRPMILES) AS S, (CASE WHEN TDAYDATE % 100 = 2 THEN 28 \
                                            WHEN TDAYDATE %100 IN (4, 6, 9, 11) THEN 30 \
                                            ELSE 31 \
                                       END) as daysInMonth \
           FROM (SELECT TRPMILES, TDAYDATE, HOUSEID, PERSONID\
                 FROM daytrip \
                 WHERE TRPMILES > 0) greaterThanZero \
           GROUP BY HOUSEID, PERSONID, TDAYDATE)SUMS;"

    cur.execute(query)
    percent = cur.fetchone()
    print i
    print percent 


conn.commit()

