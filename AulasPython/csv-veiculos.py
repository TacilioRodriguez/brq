import connections
import pandas as pd

sql = connections.getMySQL()
dfLecsv = pd.read_csv("/home/virtual/Downloads/veiculos.csv", index_col=False, delimiter = ',')
#print(lecsv)
mycursor = sql.cursor()


for row in dfLecsv.iterrows():
    id_veiculo = str (row[1] [0])
    dta_aquisicao = str (row[1] [1])
    ano_veiculo = str (row[1] [2])
    dsc_modelo = str (row[1] [3])
    num_placa = str (row[1] [4])
    status = str (row[1] [5])
    vlr_diaria = str (row[1] [6])
    insertquery = """INSERT INTO tacilio_rodrigues.VEICULOS 
    (id_veiculo, dta_aquisicao, ano_veiculo, dsc_modelo, num_placa, status, vlr_diaria) 
    VALUES ({},'{}', {}, '{}', '{}', '{}', {})""".format(id_veiculo, dta_aquisicao, ano_veiculo, dsc_modelo, num_placa, status, vlr_diaria)
    print(insertquery)
    mycursor.execute(insertquery)
    sql.commit()


