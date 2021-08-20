from cassandra.cluster import Cluster #importando somente um item do modulo
import pandas as pd #importando o modulo inteiro
#import MySQLdb 
import connections 

# def getConnMySQL():
#     connMySQL = MySQLdb.connect(
#         host='177.104.61.65',
#         user='fgv',
#         passwd='fgv',
#         db='stockfgv'
#     )
#     return connMySQL

#chama a conexao com o banco
connMySQL = connections.getConnMySQL()
session = connections.getConnCassandra()


def readFromMySql ():
    dfStockTacilio = pd.read_sql ("""SELECT u.username,
                                            s.date_,
                                            s.symbol,
                                            s.volume,
                                            p.shares,
                                            s.close
                                            FROM stockfgv.portfolio p 
                                            inner join stockfgv.stocks s
                                            on p.symbol = s.symbol 
                                            inner join stockfgv.users u
                                            on p.user_id = u.id 
                                            where s.date_ = '2020-05-22'""", connMySQL)
    return dfStockTacilio

def insertCassandra (dfStockTacilio):
    #session = connections.getConnCassandra()
    for row in dfStockTacilio.iterrows():
        data = str(row[1]["date_"])
        nome = str(row[1]["username"])
        simbolo = str(row[1]["symbol"])
        volume = str(row[1]["volume"])
        close = str(row[1]["close"])
        incluir = "INSERT INTO tacilio_pereira.portifolio_tacilio (id, Data, Nome, Simbolo, Volume, Close) VALUES (uuid(),'{}','{}','{}',{},{})".format(data, nome, simbolo, volume, close)
        print(incluir)
        session.execute(incluir)


dfStockTacilio = readFromMySql()
insertCassandra( dfStockTacilio )

print(__name__)

#encapular o os metodos
def main():
    dfStockTacilio = readFromMySql()
    insertCassandra( dfStockTacilio )
