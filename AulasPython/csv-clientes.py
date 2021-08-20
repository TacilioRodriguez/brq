import connections
import pandas as pd

sql = connections.getMySQL()
dfCsvCliente = pd.read_csv("/home/virtual/Downloads/clientes.csv", index_col=False, delimiter = ',')
#print(lecsv)
curclient = sql.cursor()



for row in dfCsvCliente.iterrows():
    id_cliente =        (str (row[1] [0]))
    cpf =               (str (row[1] [1]))
    cnh =               (str (row[1] [2]))
    validadecnh =       (str (row[1] [3]))
    nome =              (str (row[1] [4]))
    dtacadastro =       (str (row[1] [5]))
    dtanascimento =     (str (row[1] [6]))
    telefone =          (str (row[1] [7]))
    status =            (str (row[1] [8]))
    insertquery = """INSERT INTO tacilio_rodrigues.CLIENTES 
    (id_cliente, cpf, cnh, validadecnh, nome, dtacadastro, dtanascimento, telefone, status) 
    VALUES ({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(id_cliente, cpf, cnh, validadecnh, nome, dtacadastro, dtanascimento, telefone, status)
    print(insertquery)
    curclient.execute(insertquery)
    sql.commit()
