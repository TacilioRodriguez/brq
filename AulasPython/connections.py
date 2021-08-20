import MySQLdb 
from cassandra.cluster import Cluster
from pyhive import hive
from pymongo import MongoClient


def getConnMySQL():
    #print(__name__)
    connMySQL = MySQLdb.connect(
        host='177.104.61.65',
        user='fgv',
        passwd='fgv',
        db='stockfgv'
    )
    return connMySQL



def getMySQL():
    mySQL = MySQLdb.connect(
        host='ffborelli.ddns.net',
        user='root',
        passwd='root',
        db='tacilio_rodrigues'
    )
    return mySQL

def getConnCassandra():
    cluster = Cluster(['ffborelli.ddns.net'])
    session = cluster.connect()
    return session


def getConnHive():
    #local
    connHive = hive.Connection(host='ffborelli.ddns.net', 
    port=10000, 
        username='hive', password='hive', auth='CUSTOM')
    return connHive

def getConnMongo():
    client = MongoClient('ffborelli.ddns.net', 27017)
    db = client.big_data
    collection = db.tacilio_pereira
    return client
