from re import I
from numpy.core.arrayprint import str_format
#from pyhive import hive
from datetime import datetime
import pandas as pd
import connections 


#chama a conexao com o banco
connMySQL = connections.getConnMySQL()
connHive = connections.getConnHive()

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
    #ins = "INSERT INTO big_data.tacilio_pereira values ('{}',{},{},{},{},{},{})".format(date=date, volume=volume, open=open, high=high, low=low, close=close, adjclose=adjclose)
    ins = "INSERT INTO big_data.tacilio_pereira values ('{}',{},{},{},{},{},{})".format(date, volume, open, high, low, close, adjclose)
    cur = connHive.cursor()
    cur.execute(ins)
    connHive.commit()
    print(ins)






