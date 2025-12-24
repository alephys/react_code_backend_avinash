# accounts/admin.py
from django.contrib import admin
from .models import LogEntry, LoginEntry, Topic, TopicRequest

admin.site.register(LogEntry)
admin.site.register(LoginEntry)
admin.site.register(Topic)
admin.site.register(TopicRequest)