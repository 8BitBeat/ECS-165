import psycopg2, os

conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
cur = conn.cursor()

for i in range(5,101,5):
    query = "SELECT SUM(DAYSINMONTH * TRPMILES*EPATMPG)/SUM(DAYSINMONTH * TRPMILES)::float\
    FROM(SELECT TRPMILES,EPATMPG, (CASE WHEN daytrip.TDAYDATE%100 = 2 THEN 28 \
                                                WHEN daytrip.TDAYDATE%100 IN (4, 6, 9, 11) THEN 30 \
                                                ELSE 31 \
                                           END) as DAYSINMONTH \
        FROM daytrip INNER JOIN vehicle ON daytrip.HOUSEID = vehicle.HOUSEID AND \
                                            daytrip.VEHID = vehicle.VEHID AND \
                                            daytrip.VEHID >= 1 AND \
                                            EPATMPG > 0 \
        WHERE TRPMILES >= 0 AND TRPMILES < " + str(i) +  " )AS VEHMAPPING;"
    
    cur.execute(query)
    print("Average fuel economy of all miles traveled for trips less than " + str(i) + " is: " + str(cur.fetchall()))


cur.close()
conn.close()