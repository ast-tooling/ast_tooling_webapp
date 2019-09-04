import asyncio
import json
from django.contrib.auth import get_user_model
from channels.consumer import AsyncConsumer, SyncConsumer
from channels.db import database_sync_to_async

from .models import PrePostComp
from .prepost import compare
from .prepost import sheet_requests

class CompareConsumer(AsyncConsumer):
    async def websocket_connect(self, event):
        print('connected', event)
        await self.send({
            'type': 'websocket.accept'
        })

    async def websocket_receive(self, event):
        print('receive', event)
        front_text = event.get('text', None)
        if front_text:
            arg_dict = json.loads(front_text)
            #print(arg_dict)

            ppc_obj = PrePostComp(int(arg_dict['preId']), int(arg_dict['postId']), int(arg_dict['csrId']), ssUrl=arg_dict['url'])
            await self.send({
                'type': 'websocket.send',
                'text': json.dumps('Mission Successful'),                  
            })            
            prePostDocProps = compare.QueryMongo(ppc_obj.fsidocprops, ppc_obj.coversheetDocIds, ppc_obj.arguments)
            mergedData = compare.MergeToDataFrame(prePostDocProps[0], prePostDocProps[1], ppc_obj.fsiDocumentInfo, ppc_obj.arguments, ppc_obj.service)
            compare.CreateCompareTab(mergedData[0], mergedData[1], mergedData[2], ppc_obj.arguments, ppc_obj.service)




    async def websocket_disconnect(self, event):
        print('disconnected', event)   
