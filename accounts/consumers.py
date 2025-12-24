# accounts/consumers.py (update the update_event handlers)

import json
from channels.generic.websocket import AsyncWebsocketConsumer
class AdminConsumer(AsyncWebsocketConsumer):
    
    async def connect(self):
        await self.channel_layer.group_add("admin_updates", self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard("admin_updates", self.channel_name)

    async def update_event(self, event):
        # event is what we sent via group_send:
        # { "type": "update_event", "data": {"event": "..."}, "payload": {...} }
        data = event.get("data") or {}
        payload = event.get("payload", {}) or {}
        # data might be {"event": "..."}
        evt = None
        if isinstance(data, dict):
            evt = data.get("event")
        else:
            evt = data
        await self.send(text_data=json.dumps({
            "event": evt,
            "payload": payload
        }))

class UserConsumer(AsyncWebsocketConsumer):
    
    async def connect(self):
        await self.channel_layer.group_add("user_updates", self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard("user_updates", self.channel_name)
        
    async def update_event(self, event):
        data = event.get("data") or {}
        payload = event.get("payload", {}) or {}
        evt = None
        if isinstance(data, dict):
            evt = data.get("event")
        else:
            evt = data
        await self.send(text_data=json.dumps({
            "event": evt,
            "payload": payload
        }))



# # accounts/consumers.py
# import json
# from channels.generic.websocket import AsyncWebsocketConsumer

# class AdminConsumer(AsyncWebsocketConsumer):
#     async def connect(self):
#         await self.channel_layer.group_add("admin_updates", self.channel_name)
#         await self.accept()

#     async def disconnect(self, close_code):
#         await self.channel_layer.group_discard("admin_updates", self.channel_name)

#     async def update_event(self, event):
#         await self.send(text_data=json.dumps({
#             "data": { "event": event.get("data") },
#             "payload": event.get("payload", {})
#         }))
#         """
#         event = {
#             "type": "update_event",
#             "data": "request_approved",
#             "payload": {...}
#         }
#         We convert it to:
#         {
#             "data": { "event": "request_approved" },
#             "payload": {...}
#         }
#         """


# class UserConsumer(AsyncWebsocketConsumer):
#     async def connect(self):
#         await self.channel_layer.group_add("user_updates", self.channel_name)
#         await self.accept()

#     async def disconnect(self, close_code):
#         await self.channel_layer.group_discard("user_updates", self.channel_name)

#     async def update_event(self, event):
#         await self.send(text_data=json.dumps({
#             "data": { "event": event.get("data") },
#             "payload": event.get("payload", {})
#         }))