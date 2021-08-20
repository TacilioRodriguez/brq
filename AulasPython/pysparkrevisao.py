
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('revisao').\
getOrCreate()

df = spark.read.csv('/home/virtual/Downloads/ABT.csv', \
               inferSchema=True, header=True) 

df.printSchema()
df.select('date', 'volume').show()

from pyspark.sql.functions import *

df = df.withColumn('new_column', df['volume'] * 2)
df = df.withColumn('new_column', lit(1))

#Agrupamento (groupby)
dfGroupBy = df.groupBy('date').sum()
dfGroupBy.show(3)

#ordernar (sort)
dfGroupBy.sort('sum(volume)').show(3)

#orderm (sort) descendente
dfGroupBy.sort(col('sum(volume)').desc()).show(3)

#Tratar valores nulos
df.na.drop(subset='volume')
df.na.fill('s', subset=['volume', 'date'])

#Spark SQL

from pyspark.sql import SQLContext
#mapeamento do DataFrame para uma 'tabela' SQL

df.createOrReplaceTempView('financialTable')

spark.sql("SELECT * FROM financialTable limit 10").show(5)
dfMysql = df.toPandas()

import connections
#import pandas as pd

connMySql = connections.getMySQL()
cursql = connMySql.cursor()
#dfMysql.count()
#print(dfMysql)

#for row in dfMysql.iterrows():
#    data =      str(row[1] [0])
#    volume =    str(row[1] [1])
#    open =      str(row[1] [2])
#    hight =     str(row[1] [3])
#    low =       str(row[1] [4])
#    close =     str(row[1] [5])
#    adjclose =  str(row[1] [6])
#    myinsert = """INSERT INTO tacilio_rodrigues.ABT
#    (data, volume, open, hight, low, close, adjclose)
#    VALUES ('{}','{}','{}','{}','{}','{}','{}')""".format(data, volume, open, hight, low, close, adjclose)
#    print(myinsert)
#    cursql.execute(myinsert)
#    connMySql.commit()





for row in dfMysql.iterrows():
    data =      row[1] [0]
    volume =    float(row[1] [1])
    open =      float(row[1] [2])
    hight =     float(row[1] [3])
    low =       float(row[1] [4])
    close =     float(row[1] [5])
    adjclose =  float(row[1] [6])
    myinsert = """INSERT INTO tacilio_rodrigues.ABT
    (data, volume, open, hight, low, close, adjclose)
    VALUES ('{}', {}, {}, {}, {}, {}, {} )""".format(data, volume, open, hight, low, close, adjclose)
    print(myinsert)
    cursql.execute(myinsert)
    connMySql.commit()
    
    
#funcoes lambidas
    
lista = [1,2,3,4,5,6,7,8,9]

def filterList(x):
    return x > 5

filter( lambda x: x > 5 , lista ) 

filter( filterList , lista ) 

def isHighVolume(v):
    if v > 10000:
        return 'high'
    else : 
        return 'low'

from pyspark.sql.types import StringType

isHighVolumeUDF = udf( lambda x:isHighVolume(x),\
                      StringType())

df = df.withColumn('isHighVolume', \
                   isHighVolumeUDF(df['volume']) )

df.select('isHighVolume').show(3)






