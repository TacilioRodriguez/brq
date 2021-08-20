from pyspark.sql import *
from pyspark.sql.functions import *

# Iniciando a sessao
spark  = SparkSession.builder.appName('babies').getOrCreate()

# Lendo o arquivo
dfBabies = spark.read.csv('/home/virtual/Downloads/babies.csv', inferSchema=True, header=True)

dfBabies.createOrReplaceTempView('babies')

# Fazendo o Agrupamento dos registros e contando
dfBabiesGroup = dfBabies.groupBy("Gender", "Year").count()
dfBabies.orderBy(["Year", "Gender"], ascending=False).show()


# Verificando a quantidade de nomes repetidos no dataFrame
dfBabiesHaving = dfBabies.groupBy("Name").agg(count("Name").\
                alias("QtdName")).where(col("QtdName")>1).\
                orderBy("QtdName", ascending=False).show()


# Verificando a quantidade de nomes distintos
dfBabiesCount = dfBabies.select(countDistinct("Name"))
dfBabiesCount.show()



