import os
import io
import re
import dotenv
import pyodbc as po
import sqlite3
import csv
from datetime import datetime, timedelta
from pymongo import MongoClient
from sendMsgChatGoogle import sendMsgChatGoogle

### Variaveis do local do script e log mongodb
dirapp = os.path.dirname(os.path.realpath(__file__))

## Carrega os valores do .env
dotenvProd = os.path.join(dirapp, '.env.prod')
dotenv.load_dotenv(dotenvProd)

## funcao que retorna data e hora Y-M-D H:M:S
def obterDataHora():

    """Obtem data e hora"""

    datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return datahora


## funcao para verificar os valores do dotenv
def getValueEnv(valueEnv):
    
    """Verifica valores do dotenv passado"""
    
    v_valueEnv = os.getenv(valueEnv)
    if not v_valueEnv:

        msgLog = "Variável de ambiente '{0}' não encontrada.".format(valueEnv)
        GravaLog(msgLog, 'a')

    return v_valueEnv


## funcao de gravacao de log
def GravaLog(strValue, strAcao):
    
    """Realiza a gravacao de logs"""
    
    ## Path LogFile
    datahoraLog = datetime.now().strftime('%Y-%m-%d')
    pathLog = os.path.join(dirapp, 'log')
    pathLogFile = os.path.join(pathLog, 'loginfoDatabaseAzureSql.txt')

    if not os.path.exists(pathLog):
        os.makedirs(pathLog)
    else:
        pass

    msg = strValue
    with io.open(pathLogFile, strAcao, encoding='utf-8') as fileLog:
        fileLog.write('{0}\n'.format(strValue))

    return msg


## funcao de gravacao do csv file
def gravaCsv(v_ListValuesMongoDB):

    """Realiza a gravacao dos dados em arquivo csv"""

    pathCsv = os.path.join(dirapp, 'csv')
    pathCsvFile = os.path.join(pathCsv, 'empresas_createAt_Null.csv')

    if not os.path.exists(pathCsv):
        os.makedirs(pathCsv)
    else:
        pass

    columns = ["database", "quantidade"]
    with open(pathCsvFile, mode='w', newline='', encoding='utf-8') as arquivo:
        escritor = csv.writer(arquivo)
        escritor.writerow(columns)
        escritor.writerows(v_ListValuesMongoDB)


## funcao de conexao ao mongodb
def listDbAndCollMongoDB(p_nameCollection):

    """
    Essa funcao conecta ao mongodb.
    Lista apenas os databases com os nomes do seguinte formato: dat_NNNNN (onde NNNNN é numérico).
    Obtem quantidade de documentos por database onde o documento no documento o campo createdAt é nulo ou o campo não existe.
    Gera uma lista dos documentos por database que entram na condicao acima.
    """

    datahora = obterDataHora()
    msgLog = 'Obtendo dados estatisticos dos databases MongoDB (Inicio): {0}'.format(datahora)
    GravaLog(msgLog, 'a')

    try:

       ## variaveis de conexao
        DBUSERNAME = getValueEnv("USERNAME_MONGODB")
        DBPASSWORD = getValueEnv("PASSWORD_MONGODB")
        MONGO_HOST = getValueEnv("SERVER_MONGODB")
        DBAUTHDB   = getValueEnv("DBAUTHDB_MONGODB")
        connstr = 'mongodb://' + DBUSERNAME + ':' + DBPASSWORD + '@' + MONGO_HOST + '/' + DBAUTHDB

        ## cria lista vazia
        listReturnMongoDb = []
        #listDbsAll = []
        contadorDbs = 0

        with MongoClient(connstr) as client:
            
            #listar todos databases
            cursor = client.list_database_names()
            pattern = re.compile(r"^dat_\d{5}$")
            filtered_databases = [db for db in cursor if pattern.match(db)]

            """
            ### EXPLICACAO DA LISTAGEM DOS DATABASES CONFORME PADRAO DO NOME ###
            Expressão regular ^dat_\d{5}$:
            - ^dat_ verifica que o nome começa com "dat_".
            - \d{5} verifica que o nome tem exatamente 5 dígitos após "dat_".
            - $ garante que não há mais caracteres após os 5 dígitos.
            pattern.match(db): Filtra apenas os databases que correspondem ao padrão.
            """

            for dbname in filtered_databases:

                if re.search("^dat_", dbname) and (len(dbname) == 9):

                    ## define database para uso no processo
                    dbCurrent = client[dbname]
                    contadorDbs = contadorDbs + 1
                    
                    ## obtem dados estatisticos da collection
                    returnCollStats = dbCurrent.command("collstats", p_nameCollection)
                    v_totalDocs = returnCollStats['count']

                    if (v_totalDocs > 0):

                        stage_match = {
                            "$match": {
                                "$or": [
                                    {"createdAt": {"$eq": None}},  # createdAt é null
                                    {"createdAt": {"$exists": False}}  # createdAt não existe
                                ]
                            }
                        }

                        stage_group = {
                            "$group": {
                                "_id" : None,
                                "TotalDocs" : { "$sum": 1 }
                            }
                        }

                        
                        pipeline = [
                            stage_match,
                            stage_group
                        ]

                        results = dbCurrent[p_nameCollection].aggregate(pipeline)

                        for valores in results:
                            v_totaldocs = valores["TotalDocs"]

                            # cria lista auxiliar vazia
                            listReturnMongoDbAux = []

                            # insere valores na lista auxiliar
                            listReturnMongoDbAux.insert(0, dbname)
                            listReturnMongoDbAux.insert(1, v_totaldocs)

                            # insere na lista final
                            listReturnMongoDb.append(listReturnMongoDbAux)

                    else:
                        
                        pass

                        """
                        # cria lista auxiliar vazia
                        listReturnMongoDbAux = []

                        # insere valores na lista auxiliar
                        listReturnMongoDbAux.insert(0, dbname)
                        listReturnMongoDbAux.insert(1, 0)
                        
                        # insere na lista final
                        listDbsAll.append(listReturnMongoDbAux)
                        """

    except Exception as e:
        datahora = obterDataHora()
        msgLog = "Erro ao obter dados do MongoDB."
        msgException = "Error: {0} - {1}:\n{2}".format(msgLog, datahora, str(e))
        GravaLog(msgException, 'a')
        sendAlertExcept(msgException)

    finally:
        datahora = obterDataHora()
        msgLog = 'Obtendo dados estatisticos dos databases MongoDB (Fim): {0}'.format(datahora)
        GravaLog(msgLog, 'a')
        
    return listReturnMongoDb, contadorDbs


## Funcao de criacao do database e tabela caso nao exista
def create_tables(dbname_sqlite3):
    """
    script sql de criacao da tabela
    pode ser adicionado a criacao de mais de uma tabela
    separando os scripts por virgulas
    """

    ## SQL COMANDS
    sql_statements = [
        """
        CREATE TABLE IF NOT EXISTS "DocsCreatedAtNulo" (
            "id" INTEGER PRIMARY KEY AUTOINCREMENT,
            "Database" TEXT NOT NULL,
            "Quantidade" INTEGER NOT NULL, 
            "DataExecucao" DATETIME NOT NULL
        )
        """
    ]

    # variaveis da conexão ao database
    path_dir_db = os.path.join(dirapp, 'db')
    path_full_dbname_sqlite3 = os.path.join(path_dir_db, dbname_sqlite3)

    # cria o diretorio caso nao exista
    if not os.path.exists(path_dir_db):
        os.makedirs(path_dir_db)
    else:
        pass


    try:
        with sqlite3.connect(path_full_dbname_sqlite3) as cnxn:
            cursor = cnxn.cursor()
            for statement in sql_statements:
                cursor.execute(statement)

            cnxn.commit()
    except sqlite3.Error as e:
        datahora = obterDataHora()
        msgLog = 'Erro ao criar tabelaS SQlite3.'
        msgException = "Error: {0} - {1}:\n{2}".format(msgLog, datahora, str(e))
        GravaLog(msgException, 'a')
        sendAlertExcept(msgException)

    finally:
        msgLog = 'Criado tabelaS no database [{0}]'.format(dbname_sqlite3)
        GravaLog(msgLog, 'a')


## funcao de gravacao do resultado em sqlite3
def databaseSqlLiteTarget(v_listMongoDb):

    """
    Funcao de gravacao dos resultado obtido em tabela no SQlite3
    """

    datahora = obterDataHora()
    msgLog = 'Insert dos dados no Sqlite3 (Inicio): {}'.format(datahora)
    GravaLog(msgLog, 'a')

    dbname_sqlite3 = getValueEnv("DATABASE_TARGET_SQLITE")
    path_dir_db = os.path.join(dirapp, 'db')
    path_full_dbname_sqlite3 = os.path.join(path_dir_db, dbname_sqlite3)

    ## verifica se banco de dados existe
    # caso não exista realizada a chamada da funcao de criacao
    if not os.path.exists(path_dir_db):
        create_tables(dbname_sqlite3)
    else:
        pass

    try:
        with sqlite3.connect(path_full_dbname_sqlite3) as cnxn:
            cursor = cnxn.cursor()

            sqlcmdINSERT = """
            INSERT INTO DocsCreatedAtNulo
                ( Database, Quantidade, DataExecucao )
            VALUES
                (?, ?, datetime('now','localtime'));
            """
            cursor.executemany(sqlcmdINSERT, v_listMongoDb)
            cnxn.commit()
            
    except sqlite3.Error as e: 
        datahora = obterDataHora()
        msgLog = 'Erro ao inserir dados SQlite3.'
        msgException = "Error: {0} - {1}:\n{2}".format(msgLog, datahora, str(e))
        GravaLog(msgException, 'a')
        sendAlertExcept(msgException)

    finally: 
        datahora = obterDataHora()
        msgLog = 'Insert dos dados no Sqlite3 (Fim): {}'.format(datahora)
        GravaLog(msgLog, 'a')


## funcao de envio de alerta ao google chat em caso de problema ativo
def sendAlertProblem(v_listMongoDb, contadorDbs):

    """
    Funcao de envio do resultado obtido como alerta via webhook ao Google Chat
    """
    
    msgReturn = ''
    qtdeDocsNull = 0
    qtdDbsDocsNull = 0
    
    for i in range(len(v_listMongoDb)):
        v_EMPRESA    = str(v_listMongoDb[i][0])
        v_QUANTIDADE = str(v_listMongoDb[i][1])
        qtdeDocsNull = qtdeDocsNull + int(v_QUANTIDADE)
        qtdDbsDocsNull = qtdDbsDocsNull + 1

        msgReturn = msgReturn + "Database: {0} - Quantidade: {1}\n"\
            .format(v_EMPRESA, v_QUANTIDADE)

    msgLog = "Total de Bancos de Dados analisados: {0}\nBancos de Dados que contem documentos com erro: {1}\nQuantidade de documentos com erro: {2}"\
        .format(contadorDbs, qtdDbsDocsNull, qtdeDocsNull)

    datahora = obterDataHora()
    msgReturn = msgReturn.rstrip('\n') ## remove quebra de linha da ultima linha
    msgWebhook = '*PROBLEMA* - DataHora verificação: {0}\nEmpresas com documentos onde createdAt é nulo ou não existe o campo\n{1}\n{2}'.\
        format(datahora, msgLog, msgReturn)
    
    GravaLog(msgWebhook, 'a')

    URL_WEBHOOK = getValueEnv("URL_WEBHOOK")
    sendMsgChatGoogle(URL_WEBHOOK, msgWebhook)

    return qtdDbsDocsNull, qtdeDocsNull



## funcao de envio de alerta ao google chat em caso de exception
def sendAlertExcept(msgException):
    
    """
    Funcao de envio de exception como alerta via webhook ao Google Chat (geralmente para um chat apenas para DBAs)
    """
    
    datahora = obterDataHora()
    
    msgReturn = msgException
    msgWebhook = '*EXCEPTION* - DataHora verificação: {0}\nEmpresas com documentos onde createdAt é nulo ou não existe o campo\n{1}'.\
            format(datahora, msgReturn)

    URL_WEBHOOK_DBA = getValueEnv("URL_WEBHOOK_DBA")
    sendMsgChatGoogle(URL_WEBHOOK_DBA, msgWebhook)



## FUNCAO INICIAL
def main():

    ## log do início da aplicacao
    datahora = obterDataHora()
    msgLog = '***** Início da aplicação: {0}'.format(datahora)
    GravaLog(msgLog, 'w')

    """Nome da colecao a ser analisada nos databases MongoDB"""
    v_nameCollection = "DocsImport"
    msgLog = "Collection Name: {0}".format(v_nameCollection)
    GravaLog(msgLog, 'a')

    v_listReturnMongoDb, v_contadorDbs = listDbAndCollMongoDB(v_nameCollection)

    if not v_listReturnMongoDb:
        datahora = obterDataHora()
        msgLog = 'Não foi possível obter dados estatísticos do MongoDB\n'
        msgLog = '{0}***** Fim da aplicação: {1}\n'.format(msgLog, datahora)
        GravaLog(msgLog, 'a')
        exit()
    else:

        ## chama funcao de insert no banco relacional
        ## passando a lista como parametro
        gravaCsv(v_listReturnMongoDb)
        databaseSqlLiteTarget(v_listReturnMongoDb)
        v_qtdDbsDocsNull, v_qtdeDocsNull = sendAlertProblem(v_listReturnMongoDb, v_contadorDbs)
        print(v_qtdeDocsNull)
    

    ## log do final da aplicacao
    datahora = obterDataHora()
    msgLog = '***** Final da aplicação: {0}'.format(datahora)
    GravaLog(msgLog, 'a')

#### inicio da aplicacao ####
if __name__ == "__main__":
    ## chamada da função inicial
    main()