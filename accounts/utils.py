# accounts/utils.py

from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
import json
import logging

logger = logging.getLogger(_name_)

def _normalize_event_arg(event_or_dict, payload=None):
    """
    Accept either:
    - event_or_dict = "event_name", payload = {...}
    - event_or_dict = {"event": "...", "payload": {...}} OR {"type":"..."} OR other dict
    Return a tuple (event_name:str, payload:dict)
    """
    if isinstance(event_or_dict, str):
        event_name = event_or_dict
        payload = payload or {}
    elif isinstance(event_or_dict, dict):
        # common shapes that callers used:
        # {"event": "...", ...} or {"type": "...", ...} or full payload
        event_name = event_or_dict.get("event") or event_or_dict.get("type") or event_or_dict.get("data") or "unknown"
        # Merge other keys (except event/type/data) into payload if payload not provided
        if payload:
            # explicit payload wins
            payload = payload
        else:
            payload = {k: v for k, v in event_or_dict.items() if k not in ("event", "type", "data")}
    else:
        event_name = "unknown"
        payload = payload or {}
    return event_name, payload

def broadcast_to_admin(event_or_dict, payload=None):
    channel_layer = get_channel_layer()
    event_name, payload = _normalize_event_arg(event_or_dict, payload)
    async_to_sync(channel_layer.group_send)(
        "admin_updates",
        {
            "type": "update_event",
            "data": {"event": event_name},
            "payload": payload or {}
        }
    )
    logger.debug("broadcast_to_admin -> event=%s payload=%s", event_name, payload)

def broadcast_to_users(event_or_dict, payload=None):
    channel_layer = get_channel_layer()
    event_name, payload = _normalize_event_arg(event_or_dict, payload)
    async_to_sync(channel_layer.group_send)(
        "user_updates",
        {
            "type": "update_event",
            "data": {"event": event_name},
            "payload": payload or {}
        }
    )
    logger.debug("broadcast_to_users -> event=%s payload=%s", event_name, payload)

# from channels.layers import get_channel_layer
# from asgiref.sync import async_to_sync

# def broadcast_to_admin(event_name, payload=None):
#     channel_layer = get_channel_layer()
#     async_to_sync(channel_layer.group_send)(
#         "admin_updates",
#         {
#             "type": "update_event",
#             "data": event_name,
#             "payload": payload or {}
#         }
#     )

# def broadcast_to_users(event_name, payload=None):
#     channel_layer = get_channel_layer()
#     async_to_sync(channel_layer.group_send)(
#         "user_updates",
#         {
#             "type": "update_event",
#             "data": event_name,
#             "payload": payload or {}
#         }
#     )