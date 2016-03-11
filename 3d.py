import psycopg2, os

conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
cur = conn.cursor()

for i in range(20, 61, 20):
	query = "SELECT DATA.YYYYMM,(((0.008887 * SUM(DAYSINMONTH*ALLGASMILES/EPATMPG)) * 117538000/COUNT(DISTINCT DATA.HOUSEID)) - (SUM((CAST(electricity.Value AS float)/CAST(mkwh.Value AS float)) * (DAYSINMONTH * EMILES/(EPATMPG * 0.090634441))) + 0.008887 * SUM(DAYSINMONTH*GMILES/EPATMPG)) * 117538000/COUNT(DISTINCT DATA.HOUSEID)) AS DELTACO2EMISSIONS \
	FROM(\
		SELECT daytrip.TDAYDATE AS YYYYMM, daytrip.HOUSEID AS HOUSEID, daytrip.VEHID AS VEHID,SUM(daytrip.TRPMILES) AS ALLGASMILES, EPATMPG, \
			(CASE WHEN SUM(daytrip.TRPMILES) <= "+str(i) +" THEN SUM(daytrip.TRPMILES) \
				WHEN SUM(daytrip.TRPMILES) > "+str(i) +" THEN "+str(i) +" \
				END) AS EMILES, \
			(CASE WHEN SUM(daytrip.TRPMILES) <= "+str(i) +" THEN 0 \
				WHEN SUM(daytrip.TRPMILES) > "+str(i) +" THEN SUM(daytrip.TRPMILES) - "+str(i) +" \
				END) AS GMILES, \
			(CASE WHEN daytrip.TDAYDATE % 100 = 2 THEN 28 \
				WHEN daytrip.TDAYDATE %100 IN (4, 6, 9, 11) THEN 30\
				ELSE 31\
				END) AS DAYSINMONTH\
		FROM vehicle NATURAL JOIN daytrip \
		WHERE daytrip.TRPMILES > 0 AND daytrip.VEHID >= 1 AND daytrip.WHODROVE = daytrip.PERSONID \
		GROUP BY YYYYMM, daytrip.HOUSEID, daytrip.VEHID, DAYSINMONTH, EPATMPG \
		ORDER BY YYYYMM, daytrip.HOUSEID \
	) AS DATA, mkwh, electricity \
	WHERE electricity.MSN = 'TXEIEUS' AND mkwh.MSN = 'ELETPUS' AND electricity.YYYYMM = DATA.YYYYMM AND mkwh.YYYYMM = DATA.YYYYMM \
	GROUP BY DATA.YYYYMM, DAYSINMONTH;"

	cur.execute(query)
	print("Change in CO2 emissions with Hybrids at Electric miles = "+ str(i) + " :")
	print cur.fetchall()

conn.commit()


# SELECT DATA.YYYYMM,((0.008887 * SUM(DAYSINMONTH*ALLGASMILES/EPATMPG)) * 117538000/COUNT(DISTINCT DATA.HOUSEID)) AS ALLGASCO2, (SUM((CAST(electricity.Value AS float)/CAST(mkwh.Value AS float)) * (DAYSINMONTH * EMILES/(EPATMPG * 0.090634441))) + 0.008887 * SUM(DAYSINMONTH*GMILES/EPATMPG)) * 117538000/COUNT(DISTINCT DATA.HOUSEID) AS TOTALHYBRIDCO2
# FROM(
# 	SELECT daytrip.TDAYDATE AS YYYYMM, daytrip.HOUSEID AS HOUSEID, daytrip.VEHID AS VEHID,SUM(daytrip.TRPMILES) AS ALLGASMILES, EPATMPG,
# 		(CASE WHEN SUM(daytrip.TRPMILES) <= 40 THEN SUM(daytrip.TRPMILES)
# 			WHEN SUM(daytrip.TRPMILES) > 40 THEN 40
# 			END) AS EMILES,
# 		(CASE WHEN SUM(daytrip.TRPMILES) <= 40 THEN 0
# 			WHEN SUM(daytrip.TRPMILES) > 40 THEN SUM(daytrip.TRPMILES) - 40
# 			END) AS GMILES,
# 		(CASE WHEN daytrip.TDAYDATE % 100 = 2 THEN 28 
# 			WHEN daytrip.TDAYDATE %100 IN (4, 6, 9, 11) THEN 30
# 			ELSE 31
# 			END) AS DAYSINMONTH
# 	FROM vehicle NATURAL JOIN daytrip
# 	WHERE daytrip.TRPMILES > 0 AND daytrip.VEHID >= 1 AND daytrip.WHODROVE = daytrip.PERSONID
# 	GROUP BY YYYYMM, daytrip.HOUSEID, daytrip.VEHID, DAYSINMONTH, EPATMPG
# 	ORDER BY YYYYMM, daytrip.HOUSEID
# ) AS DATA, mkwh, electricity
# WHERE electricity.MSN = 'TXEIEUS' AND mkwh.MSN = 'ELETPUS' AND electricity.YYYYMM = DATA.YYYYMM AND mkwh.YYYYMM = DATA.YYYYMM
# GROUP BY DATA.YYYYMM, DAYSINMONTH;
# 	