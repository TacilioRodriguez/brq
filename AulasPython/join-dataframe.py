
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('join-dataframe').\
    getOrCreate()

import connections
import pandas as pd

connMySQL = connections.getConnMySQL()

query = " SELECT * FROM fabrizio_borelli.CLIENTE "
dfPdCliente = pd.read_sql(query, connMySQL)
dfCliente = spark.createDataFrame(dfPdCliente)

dfCliente.show()

query = " SELECT * FROM fabrizio_borelli.PEDIDO "
dfPdPedido = pd.read_sql(query, connMySQL)
dfPedido = spark.createDataFrame(dfPdPedido)

dfPedido.show()
dfPedido.columns

dfJoin = dfCliente.join(dfPedido, 'CODCLI')

condicao = [ dfCliente['CODCLI'] == dfPedido['CODCLI'] ]
dfJoin = dfCliente.join(dfPedido, condicao, how='left')

dfJoin.columns
dfJoin.show()
