#from cassandra.cluster import Cluster
import connections
import pandas as pd
import csv

# cluster = Cluster(['ffborelli.ddns.net'])
# session = cluster.connect()

session = connections.getConnCassandra() #referenciando o modulo de connections

# csvreader = pd.read_csv("/home/virtual/Downloads/ABT.csv")

# csvfile = open('/home/virtual/Downloads/ABT.csv', "r")
# csvreader = csv.reader(csvfile)

# next(csvreader)

# for row in csvreader:
#     print(row)
#     query = "INSERT INTO big_data.tacilio_pereira (id, date, volume, open, high, low, close, adjclose) VALUES (uuid(), '{}',{},{}, {}, {} , {} , {})".format(row[0], row[1] , row[2], row[3], row[4], row[5], row[6])
#     print(query)
#     session.execute(query)


# from cassandra.cluster import Cluster

# cluster = Cluster(['ffborelli.ddns.net'])
# session = cluster.connect()
#print(session)

# rows = session.execute("Select * from fabrizio_borelli.student")
# print(rows)

# for row in rows:
#     print(row)

## sol 1


path_csv = '/home/virtual/Downloads/ABT.csv'
csvfile = open(path_csv, 'r')
csvReader = csv.reader(csvfile)


next(csvReader)




for row in csvReader:
    #print(row)
    data = row[0]
    volume = row[1]
    open = row[2]
    high= row[3]
    low=row[4]
    close=row[5]
    adjclose=row[6]
    query = "Insert into big_data.tacilio_pereira (id, date , volume , open , high , low, close, adjclose ) VALUES ( uuid(), '{}', {} ,{} , {} ,{} , {} , {} )".format( data, volume, open, high, low, close, adjclose )
    print(query)
    session.execute(query)

# csvreader = connCsv()
# incluirCassandra (csvreader)