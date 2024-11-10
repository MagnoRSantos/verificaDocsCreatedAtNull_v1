# -*- coding: utf-8 -*-

import os, io
from datetime import datetime
from json import dumps
from httplib2 import Http

## funcao de envio de dados ao chat google via webhook
def sendMsgChatGoogle(urlWebhook: str, msgWebhook: str):
    
    try:
        url = urlWebhook

        message = {
            'text' : msgWebhook
        }

        message_headers = {'Content-Type': 'application/json; charset=UTF-8'}

        http_obj = Http()

        http_obj.request(
            uri=url,
            method='POST',
            headers=message_headers,
            body=dumps(message),
        )

        """
        response = http_obj.request(
            uri=url,
            method='POST',
            headers=message_headers,
            body=dumps(message),
        )

        #print(response)
        """
    except Exception as e:
        datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msgLog = "Error ao enviar dados ao Google Chat via Webhook."
        msgException = "Error: {0} - {1}:\n{2}".format(msgLog, datahora, str(e))
        GravaLog(msgException, 'a')


## funcao de gravacao de log
def GravaLog(strValue, strAcao):
    
    """Realiza a gravacao de logs"""
    
    ## Path LogFile
    ### Variaveis do local do script e log mongodb
    dirapp = os.path.dirname(os.path.realpath(__file__))
    pathLog = os.path.join(dirapp, 'log')
    pathLogFile = os.path.join(pathLog, 'logAPP.txt')

    if not os.path.exists(pathLog):
        os.makedirs(pathLog)
    else:
        pass

    msg = strValue
    with io.open(pathLogFile, strAcao, encoding='utf-8') as fileLog:
        fileLog.write('{0}\n'.format(strValue))

    return msg