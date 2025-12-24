from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone

class LogEntry(models.Model):
    command = models.CharField(max_length=255)
    approved = models.BooleanField(default=False)
    message = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.command} - {self.created_at}"

class LoginEntry(models.Model):
    username = models.CharField(max_length=255)
    login_time = models.DateTimeField(auto_now_add=True)
    success = models.BooleanField(default=False)

    def __str__(self):
        return f"{self.username} - {self.login_time} - {'Success' if self.success else 'Failed'}"

class Topic(models.Model):

    name = models.CharField(max_length=255, unique=True)
    partitions = models.PositiveIntegerField(default=3)
    created_by = models.ForeignKey(User, on_delete=models.CASCADE)
    created_at = models.DateTimeField(default=timezone.now)
    is_active = models.BooleanField(default=True)
    production = models.CharField(max_length=20, default="Inactive")
    consumption = models.CharField(max_length=20, default="Inactive")
    followers = models.IntegerField(default=0)
    observers = models.IntegerField(default=0)
    last_produced = models.DateTimeField(null=True, blank=True)
    replications = models.IntegerField(default=3)
    

    def __str__(self):
        return self.name

# class TopicRequest(models.Model):
#     STATUS_CHOICES = [
#         ('PENDING', 'Pending'),
#         ('APPROVED', 'Approved'),
#         ('DECLINED', 'Declined'),
#     ]
#     topic_name = models.CharField(max_length=255)
#     partitions = models.PositiveIntegerField(default=3)
#     requested_by = models.ForeignKey(User, on_delete=models.CASCADE)
#     request_type = models.CharField(max_length=20, choices=[('creation', 'Creation'), ('deletion', 'Deletion')],default='creation')
#     requested_at = models.DateTimeField(auto_now_add=True)
#     status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='PENDING')
#     reviewed_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name='reviewed_requests')
#     reviewed_at = models.DateTimeField(null=True, blank=True)

#     def __str__(self):
#         return f"{self.topic_name} - {self.status}"

class TopicRequest(models.Model):
    REQUEST_TYPES = (
        ("CREATE", "Create"),
        ("DELETE", "Delete"),
        ("ALTER", "Alter"),
    )

    topic = models.ForeignKey(
        Topic, on_delete=models.CASCADE, null=True, blank=True
    )
    topic_name = models.CharField(max_length=255)
    requested_by = models.ForeignKey(User, on_delete=models.CASCADE)

    request_type = models.CharField(max_length=10, choices=REQUEST_TYPES)

    # 🔹 ALTER FIELDS
    new_partitions = models.IntegerField(null=True, blank=True)
    new_replication_factor = models.IntegerField(null=True, blank=True)

    status = models.CharField(
        max_length=10,
        choices=(
            ("PENDING", "Pending"),
            ("APPROVED", "Approved"),
            ("DECLINED", "Declined"),
        ),
        default="PENDING",
    )
    
    decline_reason = models.CharField(
    max_length=255,
    null=True,
    blank=True
)

    created_at = models.DateTimeField(auto_now_add=True)

    
