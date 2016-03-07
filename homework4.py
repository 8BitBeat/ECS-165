import csv
import psycopg2
import os

count = 0
prev_msn = ""

try:
    conn = psycopg2.connect("dbname=postgres host=/home/" + os.environ['USER'] + "/postgres")
except:
    print("didnt work")

try:
    cur = conn.cursor()
except:
    print("didnt work 2")

try:
    cur.execute("CREATE TABLE electricity( MSN varchar(7), YYYYMM int, Value varchar(13));")
except:
    print"dun work"

with open('EIA_CO2_Electricity_2015.csv', 'r') as csvfile:
    
    # try:
    #     cur.execute("CREATE TABLE electricity( MSN varchar(7), YYYYMM int, Value varchar(13));")
    # except:
    #     print "bad execution 1"
    # try:
    #     cur.execute("CREATE TABLE electricity_misc(MSN varchar(7), Column_Order varchar(2), Description varchar(55), Unit varchar(39));")
    # except:
    #     print "bad exec 2"

    reader = csv.reader(csvfile)
    headings = reader.next()
    query = 'INSERT INTO electricity ('+ headings[0] +', ' + headings[1] + ', ' + headings[2] + ' ) VALUES '
    misc_query = 'INSERT INTO electricity_misc ('+ headings[0] +', ' + headings[3] + ', ' + headings[4] + ', ' + headings[5] +  ' ) VALUES '
    
    
    for row in reader:
        query+='('+ row[0] +', ' + row[1] + ', ' + row[2] + ' ), ' 
        #when there is a new MSN we insert into the misc relation
        if(row[0] != prev_msn):
            prev_msn = row[0]
            misc_query+='('+ row[0] +', ' + row[3] + ', ' + row[4] + ', ' + row[5]  +' ), '
        ++count
        if count == 1000: 
            query = query[:-2] + ';'
            #insert query into table
            try:
                print "hi"
                #cur.execute(query)
            except:
                print "bad exec 3"
            query = 'INSERT INTO electricity ('+ headings[0] +', ' + headings[1] + ', ' + headings[2] + ' ) VALUES '
            count = 0
    if count > 0:
        try:
            print"stupid"
            #cur.execute(query)
        except:
            print "bad exec 4"
    misc_query = misc_query[:-2] + ';'
    #insert misc_query into table     
    #cur.execute(misc_query)   
