import connections
import pandas as pd

sql = connections.getMySQL()
dfCsvDespachante = pd.read_csv("/home/virtual/Downloads/despachantes.csv", index_col=False, delimiter = ',')
curDesp = sql.cursor()

for line in dfCsvDespachante.iterrows():
    id_despachante =        str(line[1] [0])
    dsc_nome_despachante =  str(line[1] [1])
    status =                str(line[1] [2])
    dsc_filial =            str(line[1] [3])
    insertDesp = """INSERT INTO tacilio_rodrigues.DESPACHANTES (id_despachante, dsc_nome_despachante, status, dsc_filial)
    VALUES ({},'{}','{}','{}')""".format(id_despachante, dsc_nome_despachante,status,dsc_filial)
    print(insertDesp)
    curDesp.execute(insertDesp)
    sql.commit()