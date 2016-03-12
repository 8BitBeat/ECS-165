import psycopg2, os

conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
cur = conn.cursor()

numbers = [84, 107, 208, 270]

for i in numbers:
	query = "SELECT DATA.YYYYMM,(((0.008887 * SUM(DAYSINMONTH*ALLGASMILES/EPATMPG)) * 117538000/COUNT(DISTINCT DATA.HOUSEID)) - (SUM((CAST(electricity.Value AS float)/CAST(mkwh.Value AS float)) * (DAYSINMONTH * EMILES/(EPATMPG * 0.090634441))) + 0.008887 * SUM(DAYSINMONTH*GMILES/EPATMPG)) * 117538000/COUNT(DISTINCT DATA.HOUSEID)) AS DELTACO2EMISSIONS \
	FROM(\
		SELECT daytrip.TDAYDATE AS YYYYMM, daytrip.HOUSEID AS HOUSEID, daytrip.VEHID AS VEHID, daytrip.TRPMILES AS ALLGASMILES, EPATMPG, \
			(CASE WHEN daytrip.TRPMILES <= "+str(i) +" THEN daytrip.TRPMILES \
				WHEN daytrip.TRPMILES > "+str(i) +" THEN 0 \
				END) AS EMILES, \
			(CASE WHEN daytrip.TRPMILES <= "+str(i) +" THEN 0 \
				WHEN daytrip.TRPMILES > "+str(i) +" THEN daytrip.TRPMILES \
				END) AS GMILES, \
			(CASE WHEN daytrip.TDAYDATE % 100 = 2 THEN 28 \
				WHEN daytrip.TDAYDATE %100 IN (4, 6, 9, 11) THEN 30\
				ELSE 31\
				END) AS DAYSINMONTH\
		FROM vehicle NATURAL JOIN daytrip \
		WHERE daytrip.TRPMILES > 0 AND daytrip.VEHID >= 1 AND daytrip.WHODROVE = daytrip.PERSONID \
		ORDER BY YYYYMM, daytrip.HOUSEID \
	) AS DATA, mkwh, electricity \
	WHERE electricity.MSN = 'TXEIEUS' AND mkwh.MSN = 'ELETPUS' AND electricity.YYYYMM = DATA.YYYYMM AND mkwh.YYYYMM = DATA.YYYYMM \
	GROUP BY DATA.YYYYMM, DAYSINMONTH;"

	cur.execute(query)
	print("Change in CO2 emissions with Hybrids at Electric miles = "+ str(i) + " :")
	print cur.fetchall()

conn.commit()