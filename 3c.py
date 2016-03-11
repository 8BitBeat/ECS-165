import psycopg2
import os

conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
cur = conn.cursor()

#SUM(TRPMILES/EIADMPG)*conversion*days in month*totalhouseholds/households in month, grouped by houseid and tdaydate

SELECT totalCO2/(CAST(Value AS float)*1000000) AS percentage , TDAYDATE
FROM (SELECT SUM(GALLONS) * .008887 * daysInMonth * 117538000/COUNT(DISTINCT HOUSEID) AS totalCO2, TDAYDATE
	  FROM (SELECT DISTINCT ON(TDTRPNUM, daytrip.HOUSEID, daytrip.VEHID) (daytrip.TRPMILES/vehicle.EPATMPG) AS GALLONS, daytrip.TDAYDATE as TDAYDATE, daytrip.HOUSEID AS HOUSEID, (CASE WHEN daytrip.TDAYDATE % 100 = 2 THEN 28 
					                                                                                                       WHEN daytrip.TDAYDATE %100 IN (4, 6, 9, 11) THEN 30 
					                                                                                                       ELSE 31 
					                                                                                                       END) as daysInMonth 
    	  FROM daytrip, vehicle
    	  WHERE daytrip.HOUSEID = vehicle.HOUSEID AND daytrip.VEHID = vehicle.VEHID AND daytrip.TRPMILES >= 0) DATA
      GROUP BY  TDAYDATE, daysInMonth) monthly, transportation  
WHERE YYYYMM = TDAYDATE AND MSN = 'TEACEUS';


conn.commit()

* .008887 * daysInMonth * 117538000/(SELECT COUNT(DISTINCT daytrip.HOUSEID) FROM daytrip, vehicle WHERE daytrip.HOUSEID = vehicle.HOUSEID AND daytrip.VEHID = vehicle.VEHID AND daytrip.TRPMILES >= 0)