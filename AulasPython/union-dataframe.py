
from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.types import StringType


spark = SparkSession.builder.appName('union-dataframe').\
    getOrCreate()

path = '/home/virtual/Downloads/'
    
dfAAN = spark.read.csv(path + 'AAN.csv', \
                       inferSchema=True, header=True)

dfAAN.show(3)

dfAAN = dfAAN.withColumn('symbol', lit('AAN') )
dfAAN.columns
dfAAN.show(3)


dfABT = spark.read.csv(path+'ABT.csv', \
                       inferSchema=True, header=True)


dfABT = dfABT.withColumn('symbol', lit('ABT'))
dfABT.columns

#dfAll = dfABT.select('symbol', 'date')
#dfAll = dfAll.withColumnRenamed('symbol', 'simbolo')
#dfAll.show(3)

dfUnion = dfAAN.unionAll( dfABT)
dfUnion.select('symbol').distinct().show(3)
dfUnion.show(3)


# Escrever em json -- line
dfUnion.write.json('/home/virtual/Desktop/union-all.json')


# Tratando erros
import MySQLdb
import connections
connMySQL = connections.getConnMySQL()

cursor = connMySQL.cursor()
try:
    cursor.execute(query)
    connMySQL.commit()
except (MySQLdb.Error, MySQLdb.Warning) as e:
        print('ERROR EXECUTE QUERY', e)

finally:
    connMySQL.close()
