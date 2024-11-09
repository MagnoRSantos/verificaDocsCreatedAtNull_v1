# -*- coding: utf-8 -*-

from json import dumps
from httplib2 import Http

## funcao de envio de dados ao chat google via webhook
def sendMsgChatGoogle(urlWebhook: str, msgWebhook: str):
    
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