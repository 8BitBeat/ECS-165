import csv
import psycopg2
import os

count = 0
prev_msn = ""

conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
cur = conn.cursor()

with open('EIA_CO2_Electricity_2015.csv', 'r') as csvfile:
    cur.execute("CREATE TABLE electricity( MSN varchar(20), YYYYMM int, Value varchar(20));")    
    cur.execute("CREATE TABLE electricity_misc(MSN varchar(20), Column_Order varchar(5), Description varchar(100), Unit varchar(50));")
  
    reader = csv.reader(csvfile)
    headings = reader.next()
    query = 'INSERT INTO electricity ( MSN, YYYYMM, Value ) VALUES '
    misc_query = 'INSERT INTO electricity_misc ( MSN, Column_Order, Description, Unit ) VALUES '
    
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

with open('EIA_CO2_Transportation_2015.csv', 'r') as csvfile:
    cur.execute("CREATE TABLE transportation( MSN varchar(20), YYYYMM int, Value varchar(20));")    
    cur.execute("CREATE TABLE transportation_misc(MSN varchar(20), Column_Order varchar(5), Description varchar(100), Unit varchar(50));")
  
    reader = csv.reader(csvfile)
    headings = reader.next()
    query = 'INSERT INTO transportation ( MSN, YYYYMM, Value ) VALUES '
    misc_query = 'INSERT INTO transportation_misc ( MSN, Column_Order, Description, Unit ) VALUES '
    
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

with open('EIA_MkWh_2015.csv', 'r') as csvfile:
    cur.execute("CREATE TABLE mkwh( MSN varchar(20), YYYYMM int, Value varchar(20));")    
    cur.execute("CREATE TABLE mkwh_misc(MSN varchar(20), Column_Order varchar(5), Description varchar(100), Unit varchar(50));")
  
    reader = csv.reader(csvfile)
    headings = reader.next()
    query = 'INSERT INTO mkwh ( MSN, YYYYMM, Value ) VALUES '
    misc_query = 'INSERT INTO mkwh_misc ( MSN, Column_Order, Description, Unit ) VALUES '
    
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


conn.commit()