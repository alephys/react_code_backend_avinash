# accounts/routing.py
from django.urls import path
from . import consumers

websocket_urlpatterns = [
    path("ws/admin/", consumers.AdminConsumer.as_asgi()),
    path("ws/user/", consumers.UserConsumer.as_asgi()),
]