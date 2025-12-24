from django.contrib import admin
from django.urls import path
from accounts import views

urlpatterns = [
    path('login_api/', views.login_view_api, name='login_api'),
    path('logout_api/', views.logout_view_api, name='logout_api'),
    path('home_api/', views.home_api, name='home_api'),
    path('admin_dashboard_api/', views.admin_dashboard_api, name='admin_dashboard_api'),
    path('create_topic_api/<int:request_id>/', views.create_topic_api, name='create_topic_api'),
    path('topic/<str:topic_name>/', views.topic_detail_api, name='topic_detail_api'),
    path('delete_topic/<int:topic_id>/', views.delete_topic_api, name='delete_topic_api'),
    path("alter_topic_api/<int:topic_id>/", views.alter_topic_partitions, name="alter_topic_api"),
    path("history_api/", views.history_api, name="history_api"),
    
    # path("api/topic_request/", views.topic_request, name="topic_request"),
    
    # path("api/topic_request/", views.topic_request, name="topic_request_create"),
    path("topic_request/<int:request_id>/", views.topic_request, name="topic_request_delete"),

    
    path('admin/', admin.site.urls),
    path('', views.login_view, name='root'),
    path('login/', views.login_view, name='login'),
    path('logout/', views.logout_view, name='logout'),
    path('home/', views.home, name='home'),
    path('admin_dashboard/', views.admin_dashboard, name='admin_dashboard'),
    path('home/<str:topic_name>/', views.topic_detail, name='topic_detail'),
    path('create_topic/', views.create_topic, name='create_topic'),
    path('create_topic/<int:request_id>/', views.create_topic_form, name='create_topic_form'),
    # path('create_partition/<str:topic_name>/', views.create_partition, name='create_partition'),
    path('delete_partition/<str:topic_name>/', views.delete_partition, name='delete_partition'),
    path('approve_request/<int:request_id>/', views.approve_request, name='approve_request'),
    path('decline_request/<int:request_id>/', views.decline_request, name='decline_request'),
    path('delete_topic/', views.delete_topic, name='delete_topic'),
]