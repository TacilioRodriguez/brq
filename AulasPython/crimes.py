from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('crime').getOrCreate()

dfCrime = spark.read.csv('/home/virtual/Downloads/US_Crime_Rates_1960_2014.csv', inferSchema=True, header=True)

dfCrime.createOrReplaceTempView('crimes')

# Extraindo a decada da coluna Year
dfCrime = dfCrime.withColumn('Decada', (floor(col("Year")/10)*10).cast("int"))

# Somando a Violent e agrupando por Decada 
dfCrime = dfCrime.groupBy("Decada").sum("Violent")

# Ordenando pela Coluna Decada
dfCrime.orderBy(["Decada", "sum(Violent)"], ascending=False).show()
