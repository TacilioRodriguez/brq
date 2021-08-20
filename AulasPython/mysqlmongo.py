from pymongo import MongoClient
import MySQLdb 
import pandas as pd

client = MongoClient('ffborelli.ddns.net', 27017)

# print(client)

db = client.big_data

collection = db.tacilio_pereira

def getConnMySQL():
    connMySQL = MySQLdb.connect(
        host='177.104.61.65',
        user='fgv',
        passwd='fgv',
        db='stockfgv'
    )
    return connMySQL

#chama a conexao com o banco
connMySQL = getConnMySQL()

#seleciona os dados e armazena no data Frame (df)
dfStocksFgv = pd.read_sql ("SELECT date_, volume, open, close, high, low, adjclose FROM stockfgv.stocks limit 15", connMySQL)


for row in dfStocksFgv.iterrows():
    date = str(row[1]["date_"])
    volume = str(row[1]["volume"])
    open = str(row[1]["open"])
    high = str(row[1]["high"])
    low = str(row[1]["low"])
    close = str(row[1]["close"])
    adjclose = str(row[1]["adjclose"])
    objs = (
    {
        "date": date,
        "volume": volume,
        "open": open,
        "high": high,
        "low": low,
        "close": close,
        "adjclose": adjclose
    }
    )
    collection.insert(objs)
    print(objs)