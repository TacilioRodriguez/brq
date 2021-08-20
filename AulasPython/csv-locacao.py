import connections
import pandas as pd

sql = connections.getMySQL()
dfCsvLocacao = pd.read_csv("/home/virtual/Downloads/locacao.csv", index_col=False, delimiter = ',')
curLoc = sql.cursor()

for l in dfCsvLocacao.iterrows():
    id_locacao =            str(l[1] [0])
    id_cliente =            str(l[1] [1])
    id_despachante =        str(l[1] [2])
    id_veiculo =            str(l[1] [3])
    dta_inicio_locacao =    str(l[1] [4])
    dta_entrega_locacao =   str(l[1] [5])
    total =                 str(l[1] [6])
    insertLoc = """INSERT INTO tacilio_rodrigues.LOCACAO 
        (id_locacao, id_cliente, id_despachante, id_veiculo, dta_inicio_locacao, dta_entrega_locacao, total)
        VALUES({},{},{},{},'{}','{}',{})""".format(id_locacao, id_cliente, id_despachante, id_veiculo, dta_inicio_locacao, dta_entrega_locacao, total)
    print(insertLoc)
    curLoc.execute(insertLoc)
    sql.commit()