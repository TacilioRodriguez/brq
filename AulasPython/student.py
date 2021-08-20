from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('student').getOrCreate()

dfStudent = spark.read.csv('/home/virtual/Downloads/student.csv', inferSchema=True, header=True)

# Criando coluna com base em outra utilizando condicional
dfStudent = dfStudent.withColumn("MaiorIdade", when((dfStudent.age >= 18 ), lit("S") ).\
            when((dfStudent.age < 18 ), lit("N")))
        
# Verificando a se a condicao acima esta correta
dfStudent.select(col("age"), col("MaiorIdade")).show()


dfStudent.show()


