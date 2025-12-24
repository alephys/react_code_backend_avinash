
from django.contrib import admin
from django.urls import path
from accounts import views

urlpatterns = [
    # API Endpoints
    path('api/login_api/', views.login_view_api, name='login_api'),
    path('api/logout_api/', views.logout_view_api, name='logout_api'),
    
    path('api/home_api/', views.home_api, name='home_api'),
    path('api/admin_dashboard_api/', views.admin_dashboard_api, name='admin_dashboard_api'),
    
    path('api/create_topic_api/<int:request_id>/', views.create_topic_api,name='create_topic_api'),
    path('api/topic/<str:topic_name>/', views.topic_detail_api, name='topic_detail_api'),
    
    path('api/delete_topic/<int:topic_id>/', views.delete_topic_api, name='delete_topic_api'),
    
    # path("request_alter_topic/<int:topic_id>/", views.request_alter_topic, name="request_alter_topic"),

    path("api/request_alter_topic/<int:topic_id>/", views.request_alter_topic),

    path('api/alter_topic_api/<int:topic_id>/', views.alter_topic_partitions, name='alter_topic_api'),
    
    path("api/history_api/", views.history_api, name="history_api"),
    
    path("api/topic_request/<int:request_id>/", views.topic_request, name="topic_request_delete"),
    path("api/topic_request/", views.topic_request, name="topic_request_create"),
    
    path("api/cluster-status/", views.kafka_cluster_status, name="kafka_cluster_status"),
    
    
    

    # Regular Django pages
    path('', views.login_view, name='root'),
    
    path('login/', views.login_view, name='login'),
    path('logout/', views.logout_view, name='logout'),
    
    path('home/', views.home, name='home'),
    path('admin_dashboard/', views.admin_dashboard, name='admin_dashboard'),
    
    path('home/<str:topic_name>/', views.topic_detail, name='topic_detail'),
    
    path('create_topic/', views.create_topic, name='create_topic'),
    path('create_topic/<int:request_id>/', views.create_topic_form, name='create_topic_form'),
    
    path('delete_partition/<str:topic_name>/', views.delete_partition, name='delete_partition'),
    
    path('api/approve_request/<int:request_id>/', views.approve_request, name='approve_request'),
    path('api/decline_request/<int:request_id>/', views.decline_request, name='decline_request'),
    path('delete_topic/', views.delete_topic, name='delete_topic'),

    path('admin/', admin.site.urls),
]
