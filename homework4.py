import csv
import psycopg2
import os

prev_msn = ""

conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
cur = conn.cursor()

with open('/home/cjnitta/ecs165a/EIA_CO2_Electricity_2015.csv', 'r') as csvfile:
	cur.execute("CREATE TABLE electricity( MSN varchar(20), YYYYMM int, Value varchar(20));")    
	cur.execute("CREATE TABLE electricity_misc(MSN varchar(20), Column_Order varchar(5), Description varchar(100), Unit varchar(50));")
  
	reader = csv.reader(csvfile)
	headings = reader.next()
	query = 'INSERT INTO electricity ( MSN, YYYYMM, Value ) VALUES '
	misc_query = 'INSERT INTO electricity_misc ( MSN, Column_Order, Description, Unit ) VALUES '
	count = 0

	for row in reader:
		count+=1

		#replace not available with null 
		if row[2] == 'Not Available':
			#NOT SURE IF RIGHT
			row[2] = 'NULL'
		
		query += "('"+ row[0] +"', " + row[1] + ', ' + row[2] + ' ), ' 

		#when there is a new MSN we insert into the misc relation
		if(row[0] != prev_msn):
			prev_msn = row[0]
			misc_query+="( '"+ row[0] +"', " + row[3] + ", '" + row[4] + "', '" + row[5]  +"' ), "

		#insert every 1000 tuples into the relation
		if count == 1000: 
			query = query[:-2] + ';'
			try:
				cur.execute(query)
			except: 
				print "fail"
			query = 'INSERT INTO electricity ( '+ headings[0] +', ' + headings[1] + ', ' + headings[2] + ' ) VALUES '
			count = 0

	#insert remaining query into the table
	if count > 0:
		query = query[:-2] + ';'
		cur.execute(query)

	#insert into the misc relation
	misc_query = misc_query[:-2] + ';'
	cur.execute(misc_query)   

with open('/home/cjnitta/ecs165a/EIA_CO2_Transportation_2015.csv', 'r') as csvfile:
	cur.execute("CREATE TABLE transportation( MSN varchar(20), YYYYMM int, Value varchar(20));")    
	cur.execute("CREATE TABLE transportation_misc(MSN varchar(20), Column_Order varchar(5), Description varchar(100), Unit varchar(50));")
  
	reader = csv.reader(csvfile)
	headings = reader.next()
	query = 'INSERT INTO transportation ( MSN, YYYYMM, Value ) VALUES '
	misc_query = 'INSERT INTO transportation_misc ( MSN, Column_Order, Description, Unit ) VALUES '
	count = 0

	for row in reader:
		count+=1
		#replace not available with null
		if row[2] == 'Not Available':
			#NOT SURE IF RIGHT
			row[2] = 'NULL'
		query += "('"+ row[0] +"', " + row[1] + ', ' + row[2] + ' ), ' 

		#when there is a new MSN we insert into the misc relation
		if(row[0] != prev_msn):
			prev_msn = row[0]
			misc_query+="( '"+ row[0] +"', " + row[3] + ", '" + row[4] + "', '" + row[5]  +"' ), "

		#insert every 1000 tuples into the relation
		if count == 1000: 
			query = query[:-2] + ';'
			cur.execute(query)
			query = 'INSERT INTO transportation ( '+ headings[0] +', ' + headings[1] + ', ' + headings[2] + ' ) VALUES '
			count = 0

	#insert remaining query into the table
	if count > 0:
		query = query[:-2] + ';'
		cur.execute(query)

	#insert into the misc relation
	misc_query = misc_query[:-2] + ';'
	cur.execute(misc_query)   

with open('/home/cjnitta/ecs165a/EIA_MkWh_2015.csv', 'r') as csvfile:
	cur.execute("CREATE TABLE mkwh( MSN varchar(20), YYYYMM int, Value varchar(20));")    
	cur.execute("CREATE TABLE mkwh_misc(MSN varchar(20), Column_Order varchar(5), Description varchar(100), Unit varchar(50));")
  
	reader = csv.reader(csvfile)
	headings = reader.next()
	query = 'INSERT INTO mkwh ( MSN, YYYYMM, Value ) VALUES '
	misc_query = 'INSERT INTO mkwh_misc ( MSN, Column_Order, Description, Unit ) VALUES '
	count = 0

	for row in reader:
		count+=1
		#replace not available with null
		if row[2] == 'Not Available':
			#NOT SURE IF RIGHT
			row[2] = 'NULL'
		query += "('"+ row[0] +"', " + row[1] + ', ' + row[2] + ' ), ' 

		#when there is a new MSN we insert into the misc relation
		if(row[0] != prev_msn):
			prev_msn = row[0]
			misc_query+="( '"+ row[0] +"', " + row[3] + ", '" + row[4] + "', '" + row[5]  +"' ), "

		#insert every 1000 tuples into the relation
		if count == 1000: 
			query = query[:-2] + ';'
			cur.execute(query)
			query = 'INSERT INTO mkwh ( '+ headings[0] +', ' + headings[1] + ', ' + headings[2] + ' ) VALUES '
			count = 0

	#insert remaining query into the table
	if count > 0:
		query = query[:-2] + ';'
		cur.execute(query)

	#insert into the misc relation
	misc_query = misc_query[:-2] + ';'
	cur.execute(misc_query)   

with open('/home/cjnitta/ecs165a/HHV2PUB.CSV', 'r') as csvfile:
	print"hh"
	cur.execute("CREATE TABLE Household(HOUSEID int, VARSTRAT int, WTHHFIN varchar(20), DRVRCNT int, CDIVMSAR int, CENSUS_D int, CENSUS_R int, HH_HISP int, HH_RACE int, HHFAMINC int, HHRELATD int, HHRESP int, HHSIZE int, HHSTATE varchar(2), HHSTFIPS int, HHVEHCNT int, HOMEOWN int, HOMETYPE int, MSACAT int, MSASIZE int, NUMADLT int, RAIL int, RESP_CNT int, SCRESP int, TRAVDAY int, URBAN int, URBANSIZE int, URBRUR int, WRKCOUNT int, TDAYDATE int, FLAG100 int, LIF_CYC int, CNTTDHH int, HBHUR varchar(2), HTRESDN int, HTHTNRNT int, HTPPOPDN int, HTEEMPDN int, HBRESDN int, HBHTNRNT int, HBPPOPDN int, HH_CBSA varchar(5), HHC_MSA varchar(5));")
	
	reader = csv.reader(csvfile)
	headings = reader.next()
	query = "INSERT INTO Household(HOUSEID, VARSTRAT, WTHHFIN, DRVRCNT, CDIVMSAR, CENSUS_D, CENSUS_R, HH_HISP, HH_RACE, HHFAMINC, HHRELATD, HHRESP, HHSIZE, HHSTATE, HHSTFIPS, HHVEHCNT, HOMEOWN, HOMETYPE, MSACAT, MSASIZE, NUMADLT, RAIL, RESP_CNT, SCRESP, TRAVDAY, URBAN, URBANSIZE, URBRUR, WRKCOUNT, TDAYDATE, FLAG100, LIF_CYC, CNTTDHH, HBHUR, HTRESDN, HTHTNRNT, HTPPOPDN, HTEEMPDN, HBRESDN, HBHTNRNT, HBPPOPDN, HH_CBSA, HHC_MSA) VALUES ( "
	count = 0
		
	for row in reader:
		count += 1
		for element in row:

			if any(c.isalpha() for c in str(element)):
				query += "'" + str(element) + "', "  
			else:
				query += str(element) +', '
		query = query[:-2] + '), ('
	  
		#insert every 1000 tuples into the relation
		if count == 1000: 
			query = query[:-3] + ';'
			try:
				cur.execute(query)
			except:
				print " bad 1"
			query = "INSERT INTO Household(HOUSEID, VARSTRAT, WTHHFIN, DRVRCNT, CDIVMSAR, CENSUS_D, CENSUS_R, HH_HISP, HH_RACE, HHFAMINC, HHRELATD, HHRESP, HHSIZE, HHSTATE, HHSTFIPS, HHVEHCNT, HOMEOWN, HOMETYPE, MSACAT, MSASIZE, NUMADLT, RAIL, RESP_CNT, SCRESP, TRAVDAY, URBAN, URBANSIZE, URBRUR, WRKCOUNT, TDAYDATE, FLAG100, LIF_CYC, CNTTDHH, HBHUR, HTRESDN, HTHTNRNT, HTPPOPDN, HTEEMPDN, HBRESDN, HBHTNRNT, HBPPOPDN, HH_CBSA, HHC_MSA) VALUES ( "
			count = 0

	if count > 0:
		query = query[:-3] + ';'
		try:
			cur.execute(query)
		except:
			print "bad 2"


with open('/home/cjnitta/ecs165a/DAYV2PUB.CSV', 'r') as csvfile:
    print "Day"
    cur.execute("CREATE TABLE Daytrip(HOUSEID int, PERSONID int, FRSTHM int, OUTOFTWN int, ONTD_P1 int, ONTD_P2 int, ONTD_P3 int, ONTD_P4 int, ONTD_P5 int, ONTD_P6 int, ONTD_P7 int, ONTD_P8 int, ONTD_P9 int, ONTD_P10 int, ONTD_P11 int, ONTD_P12 int, ONTD_P13 int, ONTD_P14 int, ONTD_P15 int, TDCASEID bigint, HH_HISP int, HH_RACE int, DRIVER int, R_SEX int, WORKER int, DRVRCNT int, HHFAMINC int, HHSIZE int, HHVEHCNT int, NUMADLT int, FLAG100 int, LIF_CYC int, TRIPPURP varchar(8), AWAYHOME int, CDIVMSAR int, CENSUS_D int, CENSUS_R int, DROP_PRK int, DRVR_FLG int, EDUC int, ENDTIME int, HH_ONTD int, HHMEMDRV int, HHRESP int, HHSTATE varchar(2), HHSTFIPS int, INTSTATE int, MSACAT int, MSASIZE int, NONHHCNT int, NUMONTRP int, PAYTOLL int, PRMACT int, PROXY int, PSGR_FLG int, R_AGE int, RAIL int, STRTTIME int, TRACC1 int, TRACC2 int, TRACC3 int, TRACC4 int, TRACC5 int, TRACCTM int, TRAVDAY int, TREGR1 int, TREGR2 int, TREGR3 int, TREGR4 int, TREGR5 int, TREGRTM int, TRPACCMP int, TRPHHACC int, TRPHHVEH int, TRPTRANS int, TRVL_MIN int, TRVLCMIN int, TRWAITTM int, URBAN int, URBANSIZE int, URBRUR int, USEINTST int, USEPUBTR int, VEHID int, WHODROVE int, WHYFROM int, WHYTO int, WHYTRP1S int, WRKCOUNT int, DWELTIME int, WHYTRP90 int, TDTRPNUM int, TDWKND int, TDAYDATE int, TRPMILES int, WTTRDFIN varchar(20), VMT_MILE int, PUBTRANS int, HOMEOWN int, HOMETYPE int, HBHUR varchar(2), HTRESDN int, HTHTNRNT int, HTPPOPDN int, HTEEMPDN int, HBRESDN int, HBHTNRNT int, HBPPOPDN int, GASPRICE varchar(20), VEHTYPE int, HH_CBSA varchar(5), HHC_MSA varchar(5));")
	
    reader = csv.reader(csvfile)
    headings = reader.next()
    query = "INSERT INTO Daytrip(HOUSEID, PERSONID, FRSTHM, OUTOFTWN, ONTD_P1, ONTD_P2, ONTD_P3, ONTD_P4, ONTD_P5, ONTD_P6, ONTD_P7, ONTD_P8, ONTD_P9, ONTD_P10, ONTD_P11, ONTD_P12, ONTD_P13, ONTD_P14, ONTD_P15, TDCASEID, HH_HISP, HH_RACE, DRIVER, R_SEX, WORKER, DRVRCNT, HHFAMINC, HHSIZE, HHVEHCNT, NUMADLT, FLAG100, LIF_CYC, TRIPPURP, AWAYHOME, CDIVMSAR, CENSUS_D, CENSUS_R, DROP_PRK, DRVR_FLG, EDUC, ENDTIME, HH_ONTD, HHMEMDRV, HHRESP, HHSTATE, HHSTFIPS, INTSTATE, MSACAT, MSASIZE, NONHHCNT, NUMONTRP, PAYTOLL, PRMACT, PROXY, PSGR_FLG, R_AGE, RAIL, STRTTIME, TRACC1, TRACC2, TRACC3, TRACC4, TRACC5, TRACCTM, TRAVDAY, TREGR1, TREGR2, TREGR3, TREGR4, TREGR5, TREGRTM, TRPACCMP, TRPHHACC, TRPHHVEH, TRPTRANS, TRVL_MIN, TRVLCMIN, TRWAITTM, URBAN, URBANSIZE, URBRUR, USEINTST, USEPUBTR, VEHID, WHODROVE, WHYFROM, WHYTO, WHYTRP1S, WRKCOUNT, DWELTIME, WHYTRP90, TDTRPNUM, TDWKND, TDAYDATE, TRPMILES, WTTRDFIN, VMT_MILE, PUBTRANS, HOMEOWN, HOMETYPE, HBHUR, HTRESDN, HTHTNRNT, HTPPOPDN, HTEEMPDN, HBRESDN, HBHTNRNT, HBPPOPDN, GASPRICE, VEHTYPE, HH_CBSA, HHC_MSA) VALUES ( "
    count = 0
    altCount = 0

    for row in reader:
        count += 1
        altCount +=1
        for element in row:
            #if any element is a character we should
            if any(c.isalpha() for c in str(element)):
                query += "'" + str(element) + "', "  
            else:
                query += str(element) +', '
        query = query[:-2] + '), ('
	  
        #insert every 1000 tuples into the relation
        if count == 1000:	
            query = query[:-3] + ';'
            # print altCount
            # print query
            try:
                cur.execute(query)
            except:
                print "bad 3"

            query = "INSERT INTO Daytrip(HOUSEID, PERSONID, FRSTHM, OUTOFTWN, ONTD_P1, ONTD_P2, ONTD_P3, ONTD_P4, ONTD_P5, ONTD_P6, ONTD_P7, ONTD_P8, ONTD_P9, ONTD_P10, ONTD_P11, ONTD_P12, ONTD_P13, ONTD_P14, ONTD_P15, TDCASEID, HH_HISP, HH_RACE, DRIVER, R_SEX, WORKER, DRVRCNT, HHFAMINC, HHSIZE, HHVEHCNT, NUMADLT, FLAG100, LIF_CYC, TRIPPURP, AWAYHOME, CDIVMSAR, CENSUS_D, CENSUS_R, DROP_PRK, DRVR_FLG, EDUC, ENDTIME, HH_ONTD, HHMEMDRV, HHRESP, HHSTATE, HHSTFIPS, INTSTATE, MSACAT, MSASIZE, NONHHCNT, NUMONTRP, PAYTOLL, PRMACT, PROXY, PSGR_FLG, R_AGE, RAIL, STRTTIME, TRACC1, TRACC2, TRACC3, TRACC4, TRACC5, TRACCTM, TRAVDAY, TREGR1, TREGR2, TREGR3, TREGR4, TREGR5, TREGRTM, TRPACCMP, TRPHHACC, TRPHHVEH, TRPTRANS, TRVL_MIN, TRVLCMIN, TRWAITTM, URBAN, URBANSIZE, URBRUR, USEINTST, USEPUBTR, VEHID, WHODROVE, WHYFROM, WHYTO, WHYTRP1S, WRKCOUNT, DWELTIME, WHYTRP90, TDTRPNUM, TDWKND, TDAYDATE, TRPMILES, WTTRDFIN, VMT_MILE, PUBTRANS, HOMEOWN, HOMETYPE, HBHUR, HTRESDN, HTHTNRNT, HTPPOPDN, HTEEMPDN, HBRESDN, HBHTNRNT, HBPPOPDN, GASPRICE, VEHTYPE, HH_CBSA, HHC_MSA) VALUES ( "
            count = 0

    if count > 0:
        query = query[:-3] + ';'
        try:
            cur.execute(query)
        except:
            print "bad 4"

with open('/home/cjnitta/ecs165a/VEHV2PUB.CSV', 'r') as csvfile:
	print "vehicle"
	cur.execute("CREATE TABLE Vehicle(HOUSEID int, WTHHFIN varchar(20), VEHID int, DRVRCNT int, HHFAMINC int, HHSIZE int, HHVEHCNT int, NUMADLT int, FLAG100 int, CDIVMSAR int, CENSUS_D int, CENSUS_R int, HHSTATE varchar(2), HHSTFIPS int, HYBRID int, MAKECODE varchar(5), MODLCODE varchar(5), MSACAT int, MSASIZE int, OD_READ int, RAIL int, TRAVDAY int, URBAN int, URBANSIZE int, URBRUR int, VEHCOMM int, VEHOWNMO int, VEHYEAR int, WHOMAIN int, WRKCOUNT int, TDAYDATE int, VEHAGE int, PERSONID int, HH_HISP int, HH_RACE int, HOMEOWN int, HOMETYPE int, LIF_CYC int, ANNMILES int, HBHUR varchar(2), HTRESDN int, HTHTNRNT int, HTPPOPDN int, HTEEMPDN int, HBRESDN int, HBHTNRNT int, HBPPOPDN int, BEST_FLG int, BESTMILE int, BEST_EDT int, BEST_OUT int, FUELTYPE int, GSYRGAL int, GSCOST varchar(20), GSTOTCST int, EPATMPG int, EPATMPGF int, EIADMPG int, VEHTYPE int, HH_CBSA varchar(5), HHC_MSA varchar(5));")
	
	reader = csv.reader(csvfile)
	headings = reader.next()
	query = "INSERT INTO Vehicle(HOUSEID, WTHHFIN, VEHID, DRVRCNT, HHFAMINC, HHSIZE, HHVEHCNT, NUMADLT, FLAG100, CDIVMSAR, CENSUS_D, CENSUS_R, HHSTATE, HHSTFIPS, HYBRID, MAKECODE, MODLCODE, MSACAT, MSASIZE, OD_READ, RAIL, TRAVDAY, URBAN, URBANSIZE, URBRUR, VEHCOMM, VEHOWNMO, VEHYEAR, WHOMAIN, WRKCOUNT, TDAYDATE, VEHAGE, PERSONID, HH_HISP, HH_RACE, HOMEOWN, HOMETYPE, LIF_CYC, ANNMILES, HBHUR, HTRESDN, HTHTNRNT, HTPPOPDN, HTEEMPDN, HBRESDN, HBHTNRNT, HBPPOPDN, BEST_FLG, BESTMILE, BEST_EDT, BEST_OUT, FUELTYPE, GSYRGAL, GSCOST, GSTOTCST, EPATMPG, EPATMPGF, EIADMPG, VEHTYPE, HH_CBSA, HHC_MSA) VALUES ( "
	count = 0
	for row in reader:
		count += 1
		for element in row:		
			if any(c.isalpha() for c in str(element)):
				query += "'" + str(element) + "', "  
			else:
				query += str(element) +', '
		query = query[:-2] + '), ('
	  	
		#insert every 1000 tuples into the relation
		if count == 1000: 
			query = query[:-3] + ';'
			try:
				cur.execute(query)
			except:
				print " bad 5"
			query = "INSERT INTO Vehicle(HOUSEID, WTHHFIN, VEHID, DRVRCNT, HHFAMINC, HHSIZE, HHVEHCNT, NUMADLT, FLAG100, CDIVMSAR, CENSUS_D, CENSUS_R, HHSTATE, HHSTFIPS, HYBRID, MAKECODE, MODLCODE, MSACAT, MSASIZE, OD_READ, RAIL, TRAVDAY, URBAN, URBANSIZE, URBRUR, VEHCOMM, VEHOWNMO, VEHYEAR, WHOMAIN, WRKCOUNT, TDAYDATE, VEHAGE, PERSONID, HH_HISP, HH_RACE, HOMEOWN, HOMETYPE, LIF_CYC, ANNMILES, HBHUR, HTRESDN, HTHTNRNT, HTPPOPDN, HTEEMPDN, HBRESDN, HBHTNRNT, HBPPOPDN, BEST_FLG, BESTMILE, BEST_EDT, BEST_OUT, FUELTYPE, GSYRGAL, GSCOST, GSTOTCST, EPATMPG, EPATMPGF, EIADMPG, VEHTYPE, HH_CBSA, HHC_MSA) VALUES ( "
			count = 0

	if count > 0:
		query = query[:-3] + ';'
		try:
			cur.execute(query)
		except:
			print "bad 6"

with open('/home/cjnitta/ecs165a/PERV2PUB.CSV', 'r') as csvfile:
	print"person"
	cur.execute("CREATE TABLE Person(HOUSEID int, PERSONID int, VARSTRAT int, WTPERFIN varchar(20), SFWGT int, HH_HISP int, HH_RACE int, DRVRCNT int, HHFAMINC int, HHSIZE int, HHVEHCNT int, NUMADLT int, WRKCOUNT int, FLAG100 int, LIF_CYC int, CNTTDTR int, BORNINUS int, CARRODE int, CDIVMSAR int, CENSUS_D int, CENSUS_R int, CONDNIGH int, CONDPUB int, CONDRIDE int, CONDRIVE int, CONDSPEC int, CONDTAX int, CONDTRAV int, DELIVER int, DIARY int, DISTTOSC int, DRIVER int, DTACDT int, DTCONJ int, DTCOST int, DTRAGE int, DTRAN int, DTWALK int, EDUC int, EVERDROV int, FLEXTIME int, FMSCSIZE int, FRSTHM int, FXDWKPL int, GCDWORK varchar(20), GRADE int, GT1JBLWK int, HHRESP int, HHSTATE varchar(2), HHSTFIPS int, ISSUE int, OCCAT int, LSTTRDAY int, MCUSED int, MEDCOND int, MEDCOND6 int, MOROFTEN int, MSACAT int, MSASIZE int, NBIKETRP int, NWALKTRP int, OUTCNTRY int, OUTOFTWN int, PAYPROF int, PRMACT int, PROXY int, PTUSED int, PURCHASE int, R_AGE int, R_RELAT int, R_SEX int, RAIL int, SAMEPLC int, SCHCARE int, SCHCRIM int, SCHDIST int, SCHSPD int, SCHTRAF int, SCHTRN1 int, SCHTRN2 int, SCHTYP int, SCHWTHR int, SELF_EMP int, TIMETOSC int, TIMETOWK int, TOSCSIZE int, TRAVDAY int, URBAN int, URBANSIZE int, URBRUR int, USEINTST int, USEPUBTR int, WEBUSE int, WKFMHMXX int, WKFTPT int, WKRMHM int, WKSTFIPS int, WORKER int, WRKTIME varchar(10), WRKTRANS int, YEARMILE int, YRMLCAP int, YRTOUS int, DISTTOWK int, TDAYDATE int, HOMEOWN int, HOMETYPE int, HBHUR varchar(2), HTRESDN int, HTHTNRNT int, HTPPOPDN int, HTEEMPDN int, HBRESDN int, HBHTNRNT int, HBPPOPDN int, HH_CBSA varchar(5), HHC_MSA varchar(5));")
	
	reader = csv.reader(csvfile)
	headings = reader.next()
	query = "INSERT INTO Person(HOUSEID, PERSONID, VARSTRAT, WTPERFIN, SFWGT, HH_HISP, HH_RACE, DRVRCNT, HHFAMINC, HHSIZE, HHVEHCNT, NUMADLT, WRKCOUNT, FLAG100, LIF_CYC, CNTTDTR, BORNINUS, CARRODE, CDIVMSAR, CENSUS_D, CENSUS_R, CONDNIGH, CONDPUB, CONDRIDE, CONDRIVE, CONDSPEC, CONDTAX, CONDTRAV, DELIVER, DIARY, DISTTOSC, DRIVER, DTACDT, DTCONJ, DTCOST, DTRAGE, DTRAN, DTWALK, EDUC, EVERDROV, FLEXTIME, FMSCSIZE, FRSTHM, FXDWKPL, GCDWORK, GRADE, GT1JBLWK, HHRESP, HHSTATE, HHSTFIPS, ISSUE, OCCAT, LSTTRDAY, MCUSED, MEDCOND, MEDCOND6, MOROFTEN, MSACAT, MSASIZE, NBIKETRP, NWALKTRP, OUTCNTRY, OUTOFTWN, PAYPROF, PRMACT, PROXY, PTUSED, PURCHASE, R_AGE, R_RELAT, R_SEX, RAIL, SAMEPLC, SCHCARE, SCHCRIM, SCHDIST, SCHSPD, SCHTRAF, SCHTRN1, SCHTRN2, SCHTYP, SCHWTHR, SELF_EMP, TIMETOSC, TIMETOWK, TOSCSIZE, TRAVDAY, URBAN, URBANSIZE, URBRUR, USEINTST, USEPUBTR, WEBUSE, WKFMHMXX, WKFTPT, WKRMHM, WKSTFIPS, WORKER, WRKTIME, WRKTRANS, YEARMILE, YRMLCAP, YRTOUS, DISTTOWK, TDAYDATE, HOMEOWN, HOMETYPE, HBHUR, HTRESDN, HTHTNRNT, HTPPOPDN, HTEEMPDN, HBRESDN, HBHTNRNT, HBPPOPDN, HH_CBSA, HHC_MSA) VALUES ( "
	count = 0
	
	for row in reader:
		count += 1
		for element in row:
			if any(c.isalpha() for c in str(element)):
				query += "'" + str(element) + "', "  
			else:
				query += str(element) +', '
		query = query[:-2] + '), ('
	  
		#insert every 1000 tuples into the relation
		if count == 1000: 
			query = query[:-3] + ';'
			try:
				cur.execute(query)
			except:
				print "bad 7"
			query = "INSERT INTO Person(HOUSEID, PERSONID, VARSTRAT, WTPERFIN, SFWGT, HH_HISP, HH_RACE, DRVRCNT, HHFAMINC, HHSIZE, HHVEHCNT, NUMADLT, WRKCOUNT, FLAG100, LIF_CYC, CNTTDTR, BORNINUS, CARRODE, CDIVMSAR, CENSUS_D, CENSUS_R, CONDNIGH, CONDPUB, CONDRIDE, CONDRIVE, CONDSPEC, CONDTAX, CONDTRAV, DELIVER, DIARY, DISTTOSC, DRIVER, DTACDT, DTCONJ, DTCOST, DTRAGE, DTRAN, DTWALK, EDUC, EVERDROV, FLEXTIME, FMSCSIZE, FRSTHM, FXDWKPL, GCDWORK, GRADE, GT1JBLWK, HHRESP, HHSTATE, HHSTFIPS, ISSUE, OCCAT, LSTTRDAY, MCUSED, MEDCOND, MEDCOND6, MOROFTEN, MSACAT, MSASIZE, NBIKETRP, NWALKTRP, OUTCNTRY, OUTOFTWN, PAYPROF, PRMACT, PROXY, PTUSED, PURCHASE, R_AGE, R_RELAT, R_SEX, RAIL, SAMEPLC, SCHCARE, SCHCRIM, SCHDIST, SCHSPD, SCHTRAF, SCHTRN1, SCHTRN2, SCHTYP, SCHWTHR, SELF_EMP, TIMETOSC, TIMETOWK, TOSCSIZE, TRAVDAY, URBAN, URBANSIZE, URBRUR, USEINTST, USEPUBTR, WEBUSE, WKFMHMXX, WKFTPT, WKRMHM, WKSTFIPS, WORKER, WRKTIME, WRKTRANS, YEARMILE, YRMLCAP, YRTOUS, DISTTOWK, TDAYDATE, HOMEOWN, HOMETYPE, HBHUR, HTRESDN, HTHTNRNT, HTPPOPDN, HTEEMPDN, HBRESDN, HBHTNRNT, HBPPOPDN, HH_CBSA, HHC_MSA) VALUES ( "
			count = 0

	if count > 0:
		query = query[:-3] + ';'
		try:
			cur.execute(query)
		except:
			print "bad 8"



conn.commit()