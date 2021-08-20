from numpy.core.arrayprint import str_format
# from pyhive import hive
from datetime import datetime
import pandas as pd
import connections

# def getConnHive():
    
#     #local
#     connHive = hive.Connection(host='ffborelli.ddns.net', 
#     port=10000, 
#         username='hive', password='hive', auth='CUSTOM')
#     return connHive

# def main(args):
    
connHive = connections.getConnHive()

# print('a')
# connHive = getConnHive()
# print(connHive)

# dfHive = pd.read_sql("SELECT * FROM big_data.tacilio_pereira limit 1", connHive)
# print (datetime.now().strftime('%Y-%m-%d %H:%M:%S') + " --- {} Registros Consultados do Apache Hive ".format(dfHive.size) )
# query = 'INSERT INTO big_data.tacilio_pereira (`date`, volume, `open`, hight, low, `close`, `adjclose`) 

# pegar os dados da tabela stock_opcoes
dfStockOpcoes = pd.read_sql("SELECT * FROM big_data.stock_opcoes limit 10", connHive)

# print( dfStockOpcoes )

# processar os dados -> dobrar os valores das colunas, com excecao da data
# for dfStockOpcoes in dfStockOpcoes.iterdfStockOpcoess():
#     print(dfStockOpcoes[1]['stock_opcoes.volume'])
#     print(dfStockOpcoes[1]['stock_opcoes.data'])
#     ins = "INSERT INTO big_data.tacilio_pereira VALUES ('"+dfStockOpcoes[1]['stock_opcoes.data']+"', " + str(3 * dfStockOpcoes[1]['stock_opcoes.volume'])  + " , " + str(3 * dfStockOpcoes[1]['stock_opcoes.abertura']) + ", " + str(3* dfStockOpcoes[1]['stock_opcoes.maxima'])+ ", " +str( 3 *dfStockOpcoes[1]['stock_opcoes.minima'])+ " , " + str (3*  dfStockOpcoes[1]['stock_opcoes.fechamento'])+ " , " +str(3 *dfStockOpcoes[1]['stock_opcoes.ajuste'])+ ")" 
#     print (ins)
#     #cria o cursor e insere os dados no hive  
#     cur = connHive.cursor()
#     cur.execute(ins)
#     connHive.commit()


# pegar os dados da tabela stock_opcoes
# dfStockOpcoes = pd.read_sql("SELECT * FROM big_data.tacilio_pereira", connHive)

#print( dfStockOpcoes )

   
# aluno = 'Tacilio Rodrigues'
# vol_min = str(dfStockOpcoes['tacilio_pereira.volume'].min())
# vol_max = str(dfStockOpcoes['tacilio_pereira.volume'].max())
# abert_min = str(dfStockOpcoes['tacilio_pereira.low'].min())
# abert_max = str(dfStockOpcoes['tacilio_pereira.high'].max())
# fecha_min = str(dfStockOpcoes['tacilio_pereira.close'].min())
# fecha_max = str(dfStockOpcoes['tacilio_pereira.close'].max())


# query = "INSERT INTO big_data.estatisticas VALUES ("{aluno}", {vol_min}, {vol_max}, {abert_min}, {abert_max}, {fecha_min}, {fecha_max})".format(aluno=aluno, vol_min=vol_min, vol_max=vol_max, abert_min=abert_min, abert_max=abert_max, fecha_min=fecha_min, fecha_max=fecha_max)
# query = "INSERT INTO big_data.estatisticas VALUES ("{aluno}", {vol_min}, {vol_max}, {abert_min}, {abert_max}, {fecha_min}, {fecha_max})"

# cur = connHive.cursor()
# cur.execute(query)
# connHive.commit()
# print(query)


#forma mais simples de declarar strings para o insert no hive.
query = ('Tacilio Pereira',
          dfStockOpcoes.min()['tacilio_pereira.volume'],
          dfStockOpcoes.max()['tacilio_pereira.volume'],
          dfStockOpcoes.min()['tacilio_pereira.open'],
          dfStockOpcoes.max()['tacilio_pereira.open'],
          dfStockOpcoes.min()['tacilio_pereira.close'],
          dfStockOpcoes.max()['tacilio_pereira.close'],
          )

QUERY_INSERT = """
    INSERT INTO big_data.estatisticas
    (aluno, vol_min, vol_max, abert_min, abert_max, fecha_min, fecha_max)
     VALUES (%s, %s, %s, %s, %s, %s, %s )
    """

cur = connHive.cursor()
cur.execute( query, QUERY_INSERT )
connHive.commit()
print(query)


