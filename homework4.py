import csv

count = 0
prev_msn = ""
with open('EIA_CO2_Electricity_2015.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile)
    headings = reader.next()
    query = 'INSERT INTO Electricity ('+ headings[0] +', ' + headings[1] + ', ' + headings[2] + ' ) VALUES '
    misc_query = 'INSERT INTO ElectricityMisc ('+ headings[0] +', ' + headings[3] + ', ' + headings[4] + ', ' + headings[5] +  ' ) VALUES '
    
    
    for row in reader:
        query+='('+ row[0] +', ' + row[1] + ', ' + row[2] + ' ), ' 
        if(row[0] != prev_msn):
            prev_msn = row[0]
            misc_query+='('+ row[0] +', ' + row[3] + ', ' + row[4] + ', ' + row[5]  +' ), '



        ++count
        if count == 1000: 
            query = query[:-2]
            #insert query into table
            query = 'INSERT INTO Electricity ('+ headings[0] +', ' + headings[1] + ', ' + headings[2] + ' ) VALUES '
            count = 0
    misc_query = misc_query[:-2]
    #insert misc_query into table        
    print(misc_query)