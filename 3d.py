import psycopg2, os

conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
cur = conn.cursor()

for i in range(20, 61, 20):
	query = "SELECT DATA.YYYYMM,(((0.008887 * SUM(DAYSINMONTH*ALLGASMILES/EPATMPG)) * 117538000/COUNT(DISTINCT DATA.HOUSEID)) - (SUM((CAST(electricity.Value AS float)/CAST(mkwh.Value AS float)) * (DAYSINMONTH * EMILES/(EPATMPG * 0.090634441))) + 0.008887 * SUM(DAYSINMONTH*GMILES/EPATMPG)) * 117538000/COUNT(DISTINCT DATA.HOUSEID)) AS DELTACO2EMISSIONS \
	FROM(\
		SELECT hw4.daytrip.TDAYDATE AS YYYYMM, hw4.daytrip.HOUSEID AS HOUSEID, hw4.daytrip.VEHID AS VEHID,SUM(hw4.daytrip.TRPMILES) AS ALLGASMILES, EPATMPG, \
			(CASE WHEN SUM(hw4.daytrip.TRPMILES) <= "+str(i) +" THEN SUM(hw4.daytrip.TRPMILES) \
				WHEN SUM(hw4.daytrip.TRPMILES) > "+str(i) +" THEN "+str(i) +" \
				END) AS EMILES, \
			(CASE WHEN SUM(hw4.daytrip.TRPMILES) <= "+str(i) +" THEN 0 \
				WHEN SUM(hw4.daytrip.TRPMILES) > "+str(i) +" THEN SUM(hw4.daytrip.TRPMILES) - "+str(i) +" \
				END) AS GMILES, \
			(CASE WHEN hw4.daytrip.TDAYDATE % 100 = 2 THEN 28 \
				WHEN hw4.daytrip.TDAYDATE %100 IN (4, 6, 9, 11) THEN 30\
				ELSE 31\
				END) AS DAYSINMONTH\
		FROM hw4.vehicle NATURAL JOIN hw4.daytrip \
		WHERE hw4.daytrip.TRPMILES > 0 AND hw4.daytrip.VEHID >= 1 AND daytrip.WHODROVE = daytrip.PERSONID \
		GROUP BY YYYYMM, hw4.daytrip.HOUSEID, hw4.daytrip.VEHID, DAYSINMONTH, EPATMPG \
		ORDER BY YYYYMM, hw4.daytrip.HOUSEID \
	) AS DATA, hw4.mkwh, hw4.electricity \
	WHERE electricity.MSN = 'TXEIEUS' AND mkwh.MSN = 'ELETPUS' AND electricity.YYYYMM = DATA.YYYYMM AND mkwh.YYYYMM = DATA.YYYYMM \
	GROUP BY DATA.YYYYMM, DAYSINMONTH;"

	cur.execute(query)
	print("Change in CO2 emissions with Hybrids at Electric miles = "+ str(i) + " :")
	print cur.fetchall()

conn.commit()


# SELECT DATA.YYYYMM,((0.008887 * SUM(DAYSINMONTH*ALLGASMILES/EPATMPG)) * 117538000/COUNT(DISTINCT DATA.HOUSEID)) AS ALLGASCO2, (SUM((CAST(electricity.Value AS float)/CAST(mkwh.Value AS float)) * (DAYSINMONTH * EMILES/(EPATMPG * 0.090634441))) + 0.008887 * SUM(DAYSINMONTH*GMILES/EPATMPG)) * 117538000/COUNT(DISTINCT DATA.HOUSEID) AS TOTALHYBRIDCO2
# FROM(
# 	SELECT hw4.daytrip.TDAYDATE AS YYYYMM, hw4.daytrip.HOUSEID AS HOUSEID, hw4.daytrip.VEHID AS VEHID,SUM(hw4.daytrip.TRPMILES) AS ALLGASMILES, EPATMPG,
# 		(CASE WHEN SUM(hw4.daytrip.TRPMILES) <= 40 THEN SUM(hw4.daytrip.TRPMILES)
# 			WHEN SUM(hw4.daytrip.TRPMILES) > 40 THEN 40
# 			END) AS EMILES,
# 		(CASE WHEN SUM(hw4.daytrip.TRPMILES) <= 40 THEN 0
# 			WHEN SUM(hw4.daytrip.TRPMILES) > 40 THEN SUM(hw4.daytrip.TRPMILES) - 40
# 			END) AS GMILES,
# 		(CASE WHEN hw4.daytrip.TDAYDATE % 100 = 2 THEN 28 
# 			WHEN hw4.daytrip.TDAYDATE %100 IN (4, 6, 9, 11) THEN 30
# 			ELSE 31
# 			END) AS DAYSINMONTH
# 	FROM hw4.vehicle NATURAL JOIN hw4.daytrip
# 	WHERE hw4.daytrip.TRPMILES > 0 AND hw4.daytrip.VEHID >= 1 AND daytrip.WHODROVE = daytrip.PERSONID
# 	GROUP BY YYYYMM, hw4.daytrip.HOUSEID, hw4.daytrip.VEHID, DAYSINMONTH, EPATMPG
# 	ORDER BY YYYYMM, hw4.daytrip.HOUSEID
# ) AS DATA, hw4.mkwh, hw4.electricity
# WHERE electricity.MSN = 'TXEIEUS' AND mkwh.MSN = 'ELETPUS' AND electricity.YYYYMM = DATA.YYYYMM AND mkwh.YYYYMM = DATA.YYYYMM
# GROUP BY DATA.YYYYMM, DAYSINMONTH;
# 	