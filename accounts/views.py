import json
from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.contrib import messages
from django.utils import timezone
from .models import LogEntry, LoginEntry, Topic, TopicRequest
import logging
import re
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions

from django.views.decorators.csrf import csrf_exempt

logger = logging.getLogger(__name__)

# KAFKA_BOOTSTRAP = [
#     'avinashnode.infra.alephys.com:12091',
#     'avinashnode.infra.alephys.com:12092',
# ]

KAFKA_BOOTSTRAP = 'navyanode3.infra.alephys.com:9094'

KAFKA_CONF = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'security.protocol': 'SSL',

    'ssl.ca.location': r'C:\Users\avina\OneDrive\Desktop\test\TASK\myproject\navyaNode3\UI-broker-ca.pem',
    'ssl.certificate.location': r'C:\Users\avina\OneDrive\Desktop\test\TASK\myproject\navyaNode3\client-cert.pem',
    'ssl.key.location': r'C:\Users\avina\OneDrive\Desktop\test\TASK\myproject\navyaNode3\client-key.pem',

    'ssl.endpoint.identification.algorithm': 'none'
}


# One-time cleanup of existing topics (run once, then comment out)
# if Topic.objects.exists():
#     admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
#     for topic in Topic.objects.all():
#         try:
#             admin_client.delete_topics([topic.name])
#             logger.info(f"Cleaned up existing Kafka topic '{topic.name}'")
#         except Exception as e:
#             logger.error(f"Failed to clean up Kafka topic '{topic.name}': {e}")
#         topic.delete()
#     logger.info("All existing topics cleaned up from database")



@csrf_exempt
def login_view_api(request):
    if request.method == "POST":
        username = request.POST.get("username")
        password = request.POST.get("password")

        user = authenticate(request, username=username, password=password)

        if user is not None:
            login(request, user)
            logger.info(f"User {username} logged in successfully")

            LoginEntry.objects.create(
                username=username,
                login_time=timezone.now(),
                success=True
            )

            role = "admin" if user.is_superuser else "user"
            request.session['role'] = role # for storing role in these session
            
            # Only return JSON — no Django redirection paths
            return JsonResponse({
                "success": True,
                "message": "Login successful",
                "role": role
            })

        else:
            logger.warning(f"Failed login attempt for {username}")
            LoginEntry.objects.create(
                username=username,
                login_time=timezone.now(),
                success=False
            )
            return JsonResponse({
                "success": False,
                "message": "Invalid credentials"
            })

    return JsonResponse({"error": "Invalid request"}, status=400)


def login_view(request):
    if request.method == "POST":
        username = request.POST.get("username")
        password = request.POST.get("password")
        user = authenticate(request, username=username, password=password)
        if user is not None:
            login(request, user)
            logger.info(f"User {username} logged in successfully")
            LoginEntry.objects.create(
                username=username,
                login_time=timezone.now(),
                success=True
            )
            
            if user.is_superuser:  # store role in session based on role
                request.session['role'] = "admin"
                return JsonResponse({"success": True, "message": "Login successful", "redirect": "/admin_dashboard/"})
            else:
                request.session['role'] = "user"
                return JsonResponse({"success": True, "message": "Login successful", "redirect": "/home/"})

            
        #     if user.is_superuser:
        #         return JsonResponse({"success": True, "message": "Login successful", "redirect": "/admin_dashboard/"})
        #     return JsonResponse({"success": True, "message": "Login successful", "redirect": "/home/"})
        # else:
        #     logger.warning(f"Failed login attempt for {username}")
        #     LoginEntry.objects.create(
        #         username=username,
        #         login_time=timezone.now(),
        #         success=False
        #     )
            return JsonResponse({"success": False, "message": "Invalid credentials"})
    return render(request, "login.html")


@csrf_exempt
def logout_view_api(request):
    if request.user.is_authenticated:
        logger.info(f"User {request.user.username} logged out")
        logout(request)
        request.session.flush()
    return JsonResponse({"success": True, "message": "Logged out successfully"})


@login_required
def logout_view(request):
    if request.user.is_authenticated:
        logger.info(f"User {request.user.username} logged out")
        logout(request)
        request.session.flush()
    return redirect("login")

@csrf_exempt
def home_api(request):
    user = request.user

    if not user.is_authenticated:
        return JsonResponse(
            {"success": False, "message": "User not authenticated"},
            status=403
        )

    if request.session.get("role") != "user":
        return JsonResponse(
            {"success": False, "message": "Access denied"},
            status=403
        )

    # =======================
    # GET – User Dashboard
    # =======================
    if request.method == "GET":
        print("home_api called by:", user.username)

        created_topics = list(
            Topic.objects.filter(created_by=user).values(
                "id", "name", "partitions"
            )
        )

        uncreated_requests = list(
            TopicRequest.objects.filter(
                requested_by=user,
                # status="PENDING"
            ).values(
                "id",
                "topic_name",
                "new_partitions",
                "status",
                "request_type",
                "decline_reason",
            )
        )

        return JsonResponse({
            "success": True,
            "username": user.username,
            "created_topics": created_topics,
            "uncreated_requests": uncreated_requests,
        })

    # =======================
    # POST – Create Request
    # =======================
    elif request.method == "POST":
        try:
            data = json.loads(request.body.decode("utf-8"))
            topic_name = data.get("topic_name", "").strip()
            partitions = int(data.get("partitions", 0))

            if not topic_name or partitions < 1:
                return JsonResponse(
                    {"success": False, "message": "Invalid input"},
                    status=400
                )

            if TopicRequest.objects.filter(
                topic_name=topic_name,
                requested_by=user,
                status="PENDING"
            ).exists():
                return JsonResponse(
                    {"success": False, "message": "Request already pending"},
                    status=400
                )

            TopicRequest.objects.create(
                topic_name=topic_name,
                requested_by=user,
                request_type="CREATE",
                new_partitions=partitions,
                status="PENDING",
            )

            return JsonResponse({
                "success": True,
                "message": "Request submitted for admin approval"
            })

        except Exception as e:
            return JsonResponse(
                {"success": False, "message": str(e)},
                status=500
            )

    return JsonResponse(
        {"success": False, "message": "Unsupported method"},
        status=405
    )


# @csrf_exempt
# def home_api(request):
#     user = request.user

#     if not user.is_authenticated:
#         return JsonResponse({"success": False, "message": "User not authenticated."}, status=403)

#     if request.session.get("role") != "user":
#         return JsonResponse({"success": False, "message": "Access denied"}, status=403)

    
#     #  Handle GET requests
#     if request.method == "GET":
#         print("home_api called by:", user.username)
        
#         topics = Topic.objects.filter(created_by=user)
#         # topics = Topic.objects.filter(is_active=True, created_by=user)
#         # approved_requests = TopicRequest.objects.filter(requested_by=user, status="APPROVED")
#         # include PENDING, APPROVED, and DECLINED
        
        
#         approved_requests = TopicRequest.objects.filter(requested_by=user).exclude(status="COMPLETED")

#         # Identify approved but uncreated topics
#         approved_requests = TopicRequest.objects.filter(requested_by=user)
#         uncreated_requests = [
#             {
#                 "id": req.id,
#                 "topic_name": req.topic_name,
#                 "partitions": req.new_partitions,
#                 "status": req.status,
#                 "request_type": req.request_type,
#             }
#             for req in approved_requests
#             if req.status == "PENDING"
#         ]

#         # uncreated_requests = [
#         #     {
#         #         "id": req.id,
#         #         "topic_name": req.topic_name,
#         #         "partitions": req.partitions,
#         #         "status": req.status,
#         #     }
#         #     for req in approved_requests
#         #     # if not Topic.objects.filter(name=req.topic_name, is_active=True, created_by=user).exists()
#         #     if not Topic.objects.filter(name=req.topic_name, created_by=user).exists()
#         # ]

#         created_topics = [
#             {
#                 "id": topic.id,
#                 "name": topic.name,
#                 "partitions": topic.partitions,
#             }
#             for topic in topics
#         ]

#         # Determine user role
#         # role = "admin" if user.is_superuser else "user"
#         role = request.session.get("role")
# # The above code is using a conditional expression in Python to assign the value "admin" to
# # the variable `role` if the `user` is a superuser, otherwise it assigns the value "user" to
# # the variable `role`.


#         data = {
#             "success": True,
#             # "username": user.username,
#             # "role": role,
#             # "uncreated_requests": uncreated_requests,
#             # "created_topics": created_topics,
#             "username": user.username,
#             "uncreated_requests": uncreated_requests,
#             "created_topics": created_topics,

#         }
#         return JsonResponse(data)

#     # Handle POST requests
#     elif request.method == "POST":
#         try:
#             data = json.loads(request.body.decode("utf-8"))
#         except json.JSONDecodeError:
#             return JsonResponse({"success": False, "message": "Invalid JSON payload."}, status=400)

#         topic_name = data.get("topic_name", "").strip()
#         partitions = data.get("partitions")

#         if not topic_name or not partitions:
#             return JsonResponse({"success": False, "message": "Please fill all fields."}, status=400)

#         try:
#             partitions = int(partitions)
#             if partitions < 1:
#                 return JsonResponse({"success": False, "message": "Partitions must be at least 1."}, status=400)

#             # Prevent duplicate pending requests
#             if TopicRequest.objects.filter(topic_name=topic_name, requested_by=user, status="PENDING").exists():
#                 return JsonResponse({"success": False, "message": "You already have a pending request for this topic."}, status=400)

#             # Create new request
#             # TopicRequest.objects.create(
#             #     topic_name=topic_name,
#             #     partitions=partitions,
#             #     requested_by=user,
#             #     status="PENDING",
#             # )
            
#             TopicRequest.objects.create(
#                 topic_name=topic_name,
#                 requested_by=user,
#                 request_type="CREATE",
#                 new_partitions=partitions,
#                 status="PENDING",
#             )


#             logger.info(f"Topic request for '{topic_name}' submitted by {user.username}")
#             return JsonResponse({"success": True, "message": "Topic creation request submitted. Waiting for admin approval."})

#         except ValueError:
#             return JsonResponse({"success": False, "message": "Invalid number of partitions."}, status=400)

#     # Unsupported methods
#     return JsonResponse({"success": False, "message": "Unsupported request method."}, status=405)



@login_required
def home(request):
    if request.user.is_superuser:
        return redirect("admin_dashboard")
    context = {
        "topics": Topic.objects.filter(is_active=True, created_by=request.user),
        "username": request.user.username,
    }
    if request.method == "POST":
        topic_name = request.POST.get("topic_name").strip()
        partitions = request.POST.get("partitions")
        if topic_name and partitions:
            try:
                partitions = int(partitions)
                if partitions < 1:
                    context["error"] = "Partitions must be at least 1."
                elif TopicRequest.objects.filter(topic_name=topic_name, requested_by=request.user, status='PENDING').exists():
                    context["error"] = "You already have a pending request for this topic."
                else:
                    TopicRequest.objects.create(
                        topic_name=topic_name,
                        new_partitions=partitions,
                        requested_by=request.user
                    )
                    context["success"] = "Topic creation request submitted. Waiting for admin approval."
                    logger.info(f"Topic request for {topic_name} submitted by {request.user.username}")
            except ValueError:
                context["error"] = "Invalid number of partitions."
        else:
            context["error"] = "Please fill all fields."
    
    # Reset approved requests to ensure no stale data
    approved_requests = TopicRequest.objects.filter(requested_by=request.user, status='APPROVED')
    uncreated_requests = []
    for req in approved_requests:
        if not Topic.objects.filter(name=req.topic_name, is_active=True, created_by=request.user).exists():
            uncreated_requests.append(req)
    context["uncreated_requests"] = uncreated_requests

    # Add created topics to context
    context["created_topics"] = Topic.objects.filter(is_active=True, created_by=request.user)

    return render(request, "home.html", context)

@csrf_exempt
def admin_dashboard_api(request):
    if request.session.get("role") != "admin":
        return JsonResponse(
            {"success": False, "message": "Access denied"},
            status=403
        )

    # =======================
    # GET – Load Dashboard
    # =======================
    if request.method == "GET":
        print("admin_dashboard_api called by:", request.user.username)

        kafka_topics = get_kafka_topics(include_internal=True)
        kafka_map = {t["name"]: t for t in kafka_topics}

        db_topics = Topic.objects.all().values(
            "id", "name", "partitions", "created_by__username"
        )

        created_topics = []

        # 1️ DB topics (always NON-internal)
        for db in db_topics:
            created_topics.append({
                "id": db["id"],
                "name": db["name"],
                "partitions": db["partitions"],
                "created_by__username": db["created_by__username"],
                "is_internal": False,
            })

        # 2️ Kafka-only topics (INTERNAL)
        for name, kt in kafka_map.items():
            if not any(t["name"] == name for t in created_topics):
                created_topics.append({
                    "id": None,
                    "name": name,
                    "partitions": kt["partitions"],
                    "created_by__username": "CLI / System",
                    "is_internal": True,
                })

        return JsonResponse({
            "username": request.user.username,
            "role": "admin",
            "pending_requests": list(
                TopicRequest.objects.filter(status="PENDING")
                .order_by("-created_at")
                .values(
                    "id",
                    "topic_name",
                    "new_partitions",
                    "request_type",
                    "requested_by__username",
                )
            ),
            "created_topics": created_topics,
        })

    # =======================
    # POST – Admin creates topic
    # =======================
    elif request.method == "POST":
        try:
            data = json.loads(request.body.decode("utf-8"))
            topic_name = data.get("topic_name", "").strip()
            partitions = int(data.get("partitions", 0))

            if not topic_name or partitions < 1:
                return JsonResponse(
                    {"success": False, "message": "Invalid topic or partitions"},
                    status=400
                )

            # DB duplicate check
            if Topic.objects.filter(name=topic_name).exists():
                return JsonResponse(
                    {"success": False, "message": "Topic already exists in DB"},
                    status=400
                )

            #  KAFKA IS SOURCE OF TRUTH
            admin_client = AdminClient(KAFKA_CONF)

            # Kafka duplicate check
            existing_topics = admin_client.list_topics(timeout=10).topics
            if topic_name in existing_topics:
                return JsonResponse(
                    {"success": False, "message": "Topic already exists in Kafka"},
                    status=400
                )

            # ======================
            # CREATE TOPIC IN KAFKA
            # ======================
            new_topic = NewTopic(
                topic_name,
                num_partitions=partitions,
                replication_factor=1
            )

            fs = admin_client.create_topics([new_topic])

            for _, future in fs.items():
                future.result()  #  MUST succeed

            # ======================
            # SAVE TO DB (ONLY NOW)
            # ======================
            Topic.objects.create(
                name=topic_name,
                partitions=partitions,
                created_by=request.user,
                production="Active",
                consumption="Active",
                followers=1,
                observers=0,
                last_produced=timezone.now(),
            )

            return JsonResponse({
                "success": True,
                "message": f"Topic '{topic_name}' created successfully"
            })

        except KafkaException as e:
            logger.error(f"Kafka error: {e}")
            return JsonResponse(
                {"success": False, "message": "Kafka topic creation failed"},
                status=500
            )

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return JsonResponse(
                {"success": False, "message": str(e)},
                status=500
            )


# @csrf_exempt
# def admin_dashboard_api(request):
#     if request.session.get("role") != "admin":
#         return JsonResponse({"success": False, "message": "Access denied"}, status=403)

#     if request.method == "GET":
#         print("admin_dashboard_api called by:", request.user)
        
#         # Fetch Kafka topics
#         kafka_topics = get_kafka_topics(include_internal=True)
#         kafka_topic_names = {t["name"]: t for t in kafka_topics}

#         # Fetch DB topics
#         db_topics = Topic.objects.all().values(
#             "id", "name", "partitions", "created_by__username"
#         )

#         created_topics = []

#         # Add ALL DB topics first (NON-INTERNAL)
#         for db in db_topics:
#             created_topics.append({
#                 "id": db["id"],
#                 "name": db["name"],
#                 "partitions": db["partitions"],
#                 "created_by__username": db["created_by__username"],
#                 "is_internal": False,   #  DB topics are NEVER internal
#             })

#         #  Add Kafka-only topics (INTERNAL)
#         for kt_name, kt in kafka_topic_names.items():
#             if not any(t["name"] == kt_name for t in created_topics):
#                 created_topics.append({
#                     "id": None,
#                     "name": kt_name,
#                     "partitions": kt["partitions"],
#                     "created_by__username": "CLI / System",
#                     "is_internal": True,
#                 })

#         # Fetch all Kafka topics (including internal)
#         # kafka_topics = get_kafka_topics(include_internal=True)

#         # #  Fetch DB topics
#         # db_topics = Topic.objects.all().values(
#         #     "id", "name", "partitions", "created_by__username"
#         # )
#         # db_topics_by_name = {t["name"]: t for t in db_topics}

#         # # 3 Merge Kafka + DB topics
#         # created_topics = []
#         # for kt in kafka_topics:
#         #     db_topic = db_topics_by_name.get(kt["name"])

#         #     created_topics.append({
#         #         "id": db_topic["id"] if db_topic else None,
#         #         "name": kt["name"],
#         #         "partitions": kt["partitions"],
#         #         "created_by__username": (
#         #             db_topic["created_by__username"]
#         #             if db_topic else "CLI / System"
#         #         ),
#         #         "is_internal": kt["is_internal"]
#         #     })

#         data = {
#             "username": request.user.username,
#             "role": "admin",
#             "pending_requests": list(
#                 TopicRequest.objects.filter(status="PENDING")
#                 .order_by("-created_at")
#                 .values(
#                     "id",
#                     "topic_name",
#                     "new_partitions",
#                     "request_type",
#                     "requested_by__username",
#                 )

#             ),
#             "created_topics": created_topics,
#         }

#         return JsonResponse(data)

#     elif request.method == "POST":
#         try:
#             data = json.loads(request.body.decode("utf-8"))
#             topic_name = data.get("topic_name", "").strip()
#             partitions = data.get("partitions")

#             if not topic_name or not partitions:
#                 return JsonResponse({"success": False, "message": "Missing topic name or partitions."}, status=400)

#             partitions = int(partitions)
#             if partitions < 1:
#                 return JsonResponse({"success": False, "message": "Partitions must be at least 1."}, status=400)

#             if Topic.objects.filter(name=topic_name, is_active=True).exists():
#                 return JsonResponse({"success": False, "message": f"Topic '{topic_name}' already exists."}, status=400)

#             # Kafka topic creation
#             admin_client = AdminClient(KAFKA_CONF)
#             # admin_client = AdminClient(conf);
#             new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=1)
#             fs = admin_client.create_topics([new_topic])
#             for _, f in fs.items():
#                 f.result()

#             # Save to DB
#             Topic.objects.create(
#                 name=topic_name,
#                 partitions=partitions,
#                 created_by=request.user,
#                 production="Active",
#                 consumption="Active",
#                 followers=1,
#                 observers=0,
#                 last_produced=timezone.now(),
#             )

#             # Return updated dashboard data
#             updated_data = {
#                 "success": True,
#                 "message": f"Topic '{topic_name}' created successfully!",
#                 "pending_requests": list(
#                     TopicRequest.objects.filter(status="PENDING")
#                     .order_by("-created_at")
#                     .values("id", "topic_name", "partitions", "requested_by__username")
#                 ),
#                 "created_topics": list(
#                     # Topic.objects.filter(is_active=True)
#                     Topic.objects.all()

#                     .values("id", "name", "partitions", "created_by__username")
#                 ),
#             }

#             return JsonResponse(updated_data)

#         except Exception as e:
#             logger.error(f"Error creating topic: {e}")
#             return JsonResponse({"success": False, "message": str(e)}, status=500)


def get_kafka_topics(include_internal=True): 
    """ Returns a list of topics from Kafka: [{ "name": str, "partitions": int, "is_internal": bool }, ...] """ 
    admin_client = AdminClient(KAFKA_CONF)
    md = admin_client.list_topics(timeout=10) 
    topics = [] 
    for name, t in md.topics.items(): 
        # is_internal = name.startswith("") or name.startswith("_")
        
        # Kafka internal topics usually start with "_"
        is_internal = name.startswith("_") 
        
        if not include_internal and is_internal: 
            continue 
        topics.append( { "name": name, "partitions": len(t.partitions), "is_internal": is_internal, } ) 
    return topics

# def get_kafka_topics(include_internal=True):
#     """
#     Returns topics from Kafka:
#     [
#     { "name": str, "partitions": int, "is_internal": bool }
#     ]
#     """
#     admin_client = AdminClient(KAFKA_CONF)
#     metadata = admin_client.list_topics(timeout=10)

#     topics = []

#     for name, t in metadata.topics.items():
#         if t.error is not None:
#             continue

#         # Kafka-native internal topic flag
#         is_internal = t.is_internal

#         if not include_internal and is_internal:
#             continue

#         topics.append({
#             "name": name,
#             "partitions": len(t.partitions),
#             "is_internal": is_internal
#         })

#     return topics


# @csrf_exempt
# def admin_dashboard_api(request):

#     #  Handle GET requests
#     if request.method == "GET":
#         # We differentiate by URL: `/api/admin_dashboard`
#         print(" admin_dashboard_api called by:", request.user)
#         if "api" in request.path:
#             data = {
#                 "pending_requests": list(
#                     TopicRequest.objects.filter(status="PENDING")
#                     .order_by("-requested_at")
#                     .values("id", "topic_name", "partitions", "requested_by__username")
#                 ),
#                 "created_topics": list(
#                     Topic.objects.filter(is_active=True)
#                     .values("id", "name", "partitions", "created_by__username")
#                 ),
#                 "username": request.user.username,
#             }
#             # Return JSON for frontend API
#             return JsonResponse(data, safe=False)

#         # --- Django template rendering ---
#         # When user visits /admin_dashboard (not API)
#         context = {
#             "topics": Topic.objects.filter(is_active=True),
#             "pending_requests": TopicRequest.objects.filter(status="PENDING").order_by("-requested_at"),
#             "created_topics": Topic.objects.filter(is_active=True),
#         }
#         # Render the admin.html template
#         return render(request, "admin.html", context)

#     # Handle POST requests (Create a new Kafka topic)
#     elif request.method == "POST":
#         try:
#             #  Safely parse JSON body from React/Postman
#             data = json.loads(request.body.decode("utf-8"))
#             topic_name = data.get("topic_name", "").strip()
#             partitions = data.get("partitions")

#             # --- Basic Validation ---
#             if not topic_name or not partitions:
#                 return JsonResponse({"success": False, "message": "Missing topic name or partitions."}, status=400)

#             partitions = int(partitions)
#             if partitions < 1:
#                 return JsonResponse({"success": False, "message": "Partitions must be at least 1."}, status=400)

#             # --- Check for duplicates ---
#             if Topic.objects.filter(name=topic_name, is_active=True).exists():
#                 return JsonResponse({"success": False, "message": f"Topic '{topic_name}' already exists."}, status=400)

#             # Create topic in Kafka using confluent_kafka
#             # admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
#             admin_client = AdminClient(KAFKA_CONF)
#             new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=1)
#             admin_client.create_topics([new_topic])

#             # Store topic in Django model
#             Topic.objects.create(
#                 name=topic_name,
#                 partitions=partitions,
#                 created_by=request.user,
#                 production="Active",
#                 consumption="Active",
#                 followers=1,
#                 observers=0,
#                 last_produced=timezone.now()
#             )

#             logger.info(f"Admin created topic '{topic_name}' successfully.")
#             return JsonResponse({"success": True, "message": f"Topic '{topic_name}' created successfully!"})

#         except Exception as e:
#             logger.error(f"Error creating topic: {e}")
#             return JsonResponse({"success": False, "message": str(e)}, status=500)



@login_required
def admin_dashboard(request):
    if not request.user.is_superuser:
        return redirect("home")
    context = {
        "topics": Topic.objects.filter(is_active=True, created_by=request.user),
        "username": request.user.username,
        "pending_requests": TopicRequest.objects.filter(status='PENDING').order_by('-requested_at'),
        "created_topics": Topic.objects.filter(is_active=True),
    }
    if request.method == "POST":
        topic_name = request.POST.get("topic_name").strip()
        partitions = request.POST.get("partitions")
        if topic_name and partitions:
            try:
                partitions = int(partitions)
                if partitions < 1:
                    context["error"] = "Partitions must be at least 1."
                elif Topic.objects.filter(name=topic_name, is_active=True).exists():
                    context["error"] = f"Topic '{topic_name}' already exists."
                else:
                    try:
                        # admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
                        admin_client = AdminClient(KAFKA_CONF)
                        new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=1)
                        admin_client.create_topics([new_topic])
                        logger.info(f"Admin created Kafka topic '{topic_name}' with {partitions} partitions")
                    except Exception as e:
                        logger.error(f"Kafka topic creation failed: {e}")
                        context["error"] = f"Failed to create topic in Kafka: {str(e)}"
                        return render(request, "admin.html", context)
                    Topic.objects.create(
                        name=topic_name,
                        partitions=partitions,
                        created_by=request.user,
                        production="Active",
                        consumption="Active",
                        followers=1,
                        observers=0,
                        last_produced=timezone.now()
                    )
                    logger.info(f"Admin created topic '{topic_name}' in Django")
                    messages.success(request, f"Topic '{topic_name}' created successfully!")
                    return redirect("admin_dashboard")
            except ValueError:
                context["error"] = "Invalid number of partitions."
        else:
            context["error"] = "Please fill all fields."
    return render(request, "admin.html", context)



@csrf_exempt
def create_topic_form(request, request_id):
    try:
        topic_request = TopicRequest.objects.get(id=request_id, requested_by=request.user, status='APPROVED')
        highlight = request.GET.get('highlight', '')
        context = {
            "topic_name": topic_request.topic_name,
            "partitions": topic_request.new_partitions,
            "username": request.user.username,
            "topics": Topic.objects.filter(created_by=request.user, is_active=True),
            "request_id": request_id,
            "highlight": highlight
        }
        if request.method == "GET":
            return render(request, "create_topic.html", context)
        elif request.method == "POST":
            return create_topic(request)
    except TopicRequest.DoesNotExist:
        return render(request, "home.html", {
            "topics": Topic.objects.filter(is_active=True, created_by=request.user),
            "username": request.user.username,
            "error": "Approved request not found or you don't have permission."
        })


@csrf_exempt
def history_api(request):
    user = request.user

    if not user.is_authenticated:
        return JsonResponse({"success": False, "message": "User not authenticated."}, status=403)

    if request.method == "GET":
        print("history_api called by:", user.username)

        # If admin, show all topic requests
        if user.is_superuser:
            topic_requests = TopicRequest.objects.all().order_by("-created_at")
        else:
            topic_requests = TopicRequest.objects.filter(requested_by=user).order_by("-created_at")

        history_data = [
            {
                "id": req.id,
                "topic_name": req.topic_name,
                "partitions": req.new_partitions,
                "status": req.status,
                "requested_by": req.requested_by.username,
                "requested_at": req.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
            for req in topic_requests
        ]

        return JsonResponse(
            {"success": True, "history": history_data, "role": "admin" if user.is_superuser else "user"},
            safe=False,
        )

    return JsonResponse({"success": False, "message": "Unsupported request method."}, status=400)
        

@csrf_exempt
def create_topic_api(request, request_id):
    if request.method != "POST":
        return JsonResponse({"success": False, "message": "Invalid request method"}, status=400)

    user = request.user
    if not user.is_authenticated:
        return JsonResponse({"success": False, "message": "Unauthorized"}, status=401)

    try:
        topic_request = TopicRequest.objects.get(id=request_id, requested_by=user, status='APPROVED')
    except TopicRequest.DoesNotExist:
        return JsonResponse({"success": False, "message": "Approved request not found"}, status=404)

    topic_name = topic_request.topic_name
    partitions = topic_request.new_partitions

    try:
        # Create topic in Kafka
        # admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
        admin_client = AdminClient(KAFKA_CONF)
        new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=1)
        admin_client.create_topics([new_topic])

        # Save to DB
        Topic.objects.create(
            name=topic_name,
            partitions=partitions,
            created_by=user,
            production="Active",
            consumption="Active",
            followers=1,
            observers=0,
            last_produced=timezone.now()
        )

        # Update request status
        topic_request.status = 'PROCESSED'
        topic_request.save()

        return JsonResponse({"success": True, "message": f"Topic '{topic_name}' created successfully."})

    except Exception as e:
        logger.error(f"Kafka topic creation failed: {e}")
        return JsonResponse({"success": False, "message": f"Topic creation failed: {str(e)}"})

@csrf_exempt
def create_topic(request):
    if not request.user.is_authenticated:
        return redirect("login")
    if request.method == "POST":
        topic_name = request.POST.get("topic_name").strip()
        partitions = request.POST.get("partitions")
        request_id = request.POST.get("request_id")
        if topic_name and partitions:
            try:
                partitions = int(partitions)
                if partitions < 1:
                    return render(request, "create_topic.html", {
                        "topic_name": topic_name,
                        "partitions": partitions,
                        "username": request.user.username,
                        "topics": Topic.objects.filter(created_by=request.user, is_active=True),
                        "error": "Partitions must be at least 1."
                    })
                if not re.match(r'^[a-zA-Z0-9._-]+$', topic_name):
                    return render(request, "create_topic.html", {
                        "topic_name": topic_name,
                        "partitions": partitions,
                        "username": request.user.username,
                        "topics": Topic.objects.filter(created_by=request.user, is_active=True),
                        "error": "Topic name can only contain letters, numbers, dots, underscores, and hyphens."
                    })
                if not request.user.is_superuser:
                    approved_request = TopicRequest.objects.filter(
                        requested_by=request.user,
                        topic_name=topic_name,
                        status='APPROVED'
                    ).first()
                    if not approved_request:
                        return render(request, "home.html", {
                            "topics": Topic.objects.filter(is_active=True, created_by=request.user),
                            "username": request.user.username,
                            "error": "You need superuser approval to create this topic."
                        })
                # Check for existing topic and handle accordingly
                existing_topic = Topic.objects.filter(name=topic_name, is_active=True).first()
                if existing_topic:
                    if approved_request:
                        approved_request.status = 'PROCESSED'
                        approved_request.save()
                        logger.info(f"Request for '{topic_name}' marked as PROCESSED due to existing topic")
                    messages.warning(request, f"Topic '{topic_name}' already exists. Request marked as processed.")
                    return redirect("home")
                try:
                    # admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
                    admin_client = AdminClient(KAFKA_CONF)
                    new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=1)
                    admin_client.create_topics([new_topic])
                    logger.info(f"Kafka topic '{topic_name}' created with {partitions} partitions")
                except Exception as e:
                    logger.error(f"Kafka topic creation failed: {e}")
                    return render(request, "create_topic.html", {
                        "topic_name": topic_name,
                        "partitions": partitions,
                        "username": request.user.username,
                        "topics": Topic.objects.filter(created_by=request.user, is_active=True),
                        "error": f"Failed to create topic in Kafka: {str(e)}"
                    })
                Topic.objects.create(
                    name=topic_name,
                    partitions=partitions,
                    created_by=request.user,
                    production="Active",
                    consumption="Active",
                    followers=1,
                    observers=0,
                    last_produced=timezone.now()
                )
                if approved_request:
                    approved_request.status = 'PROCESSED'
                    approved_request.save()
                    logger.info(f"Request for '{topic_name}' marked as PROCESSED after creation")
                logger.info(f"Created topic '{topic_name}' in Django by {request.user.username}")
                messages.success(request, f"Topic '{topic_name}' created successfully!")
                return redirect("home")
            except ValueError:
                return render(request, "create_topic.html", {
                    "topic_name": topic_name,
                    "partitions": partitions,
                    "username": request.user.username,
                    "topics": Topic.objects.filter(created_by=request.user, is_active=True),
                    "error": "Invalid number of partitions."
                })
        return render(request, "create_topic.html", {
            "topic_name": topic_name,
            "partitions": partitions,
            "username": request.user.username,
            "topics": Topic.objects.filter(created_by=request.user, is_active=True),
            "error": "Please fill all fields."
        })
    return redirect("home")

# @csrf_exempt
# def delete_topic_api(request, topic_id):
#     if request.method == "DELETE":
#         try:
#             topic = Topic.objects.get(id=topic_id)
#             # admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
#             admin_client = AdminClient(KAFKA_CONF)
            
#             # Try deleting from Kafka first
#             try:
#                 admin_client.delete_topics([topic.name])
#                 logger.info(f"Kafka topic '{topic.name}' deleted by {request.user.username}")
#             except Exception as e:
#                 logger.error(f"Kafka topic deletion failed: {e}")
#                 return JsonResponse({"success": False, "message": f"Kafka deletion failed: {str(e)}"})
            
#             topic.delete()
#             logger.info(f"Topic '{topic.name}' deleted successfully from Django DB.")
#             return JsonResponse({"success": True, "message": f"Topic '{topic.name}' deleted successfully!"})
#         except Topic.DoesNotExist:
#             return JsonResponse({"success": False, "message": "Topic not found."})
    
#     return JsonResponse({"success": False, "message": "Invalid request method."})

@csrf_exempt
@login_required
def delete_topic_api(request, topic_id):
    if not request.user.is_superuser:
        return JsonResponse(
            {"success": False, "message": "Unauthorized"},
            status=403
        )

    if request.method != "DELETE":
        return JsonResponse(
            {"success": False, "message": "Invalid method"},
            status=405
        )

    try:
        topic = Topic.objects.get(id=topic_id)

        admin_client = AdminClient(KAFKA_CONF)
        metadata = admin_client.list_topics(timeout=10)

        kafka_deleted = False

        if topic.name in metadata.topics:
            try:
                fs = admin_client.delete_topics([topic.name])
                for _, f in fs.items():
                    f.result()
                kafka_deleted = True
            except Exception as e:
                return JsonResponse(
                    {
                        "success": False,
                        "message": f"Kafka deletion failed: {str(e)}"
                    },
                    status=500
                )

        # Delete from DB regardless
        topic.delete()

        if kafka_deleted:
            msg = f"Topic '{topic.name}' deleted from Kafka and DB"
        else:
            msg = (
                f"Topic '{topic.name}' deleted from DB. "
                f"Kafka topic did not exist or deletion is disabled."
            )

        return JsonResponse({"success": True, "message": msg})

    except Topic.DoesNotExist:
        return JsonResponse(
            {"success": False, "message": "Topic not found"},
            status=404
        )

    except Exception as e:
        return JsonResponse(
            {"success": False, "message": str(e)},
            status=500
        )



# alter topic 


# @csrf_exempt
# def alter_topic_partitions(request, topic_id):
#     if request.method != "PATCH":
#         return JsonResponse({"success": False, "message": "Invalid request method. Use PATCH."})

#     try:
#         topic = Topic.objects.get(id=topic_id)
#         data = json.loads(request.body.decode("utf-8"))
#         new_partition_count = data.get("new_partition_count")

#         if not new_partition_count or not isinstance(new_partition_count, int):
#             return JsonResponse({"success": False, "message": "Provide a valid integer for new_partition_count."})

#         # Connect to Kafka
#         # admin_client = AdminClient({
#         #     'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP)
#         # })
#         admin_client = AdminClient(KAFKA_CONF)
#         # Get current partitions from Kafka
#         metadata = admin_client.list_topics(timeout=10)
        
#         if topic.name not in metadata.topics:
#             return JsonResponse({
                
#                 "success": False,
#                 "message": f"Topic '{topic.name}' not found in Kafka cluster."
#         }, status=400)
            
#         current_partitions = len(metadata.topics[topic.name].partitions)

#         logger.info(f"Current partitions for '{topic.name}': {current_partitions}")

#         if new_partition_count <= current_partitions:
#             return JsonResponse({
#                 "success": False,
#                 "message": f"Cannot reduce partitions. Current: {current_partitions}, Requested: {new_partition_count}"
#             })

#         new_parts = [
#             NewPartitions(topic.name, new_total_count=new_partition_count)
#         ]
#         fs = admin_client.create_partitions(new_parts)

#         for t, f in fs.items():
#             try:
#                 f.result()  # Wait for operation completion
#                 logger.info(f"Topic '{t}' partition count increased to {new_partition_count}.")
#             except Exception as e:
#                 logger.error(f"Failed to alter partitions for {t}: {e}")
#                 return JsonResponse({"success": False, "message": f"Kafka alter failed: {str(e)}"})

#         # Update in database
#         topic.partitions = new_partition_count
#         topic.save()

#         return JsonResponse({
#             "success": True,
#             "message": f"Partitions for topic '{topic.name}' increased from {current_partitions} → {new_partition_count}"
#         })

#     except Topic.DoesNotExist:
#         return JsonResponse({"success": False, "message": "Topic not found."})
#     except Exception as e:
#         import traceback
#         logger.error(traceback.format_exc())
#         return JsonResponse({"success": False, "message": str(e)})

# @csrf_exempt
# @login_required
# def alter_topic_partitions(request, topic_id):
#     if request.method != "PATCH":
#         return JsonResponse(
#             {"success": False, "message": "Invalid request method. Use PATCH."},
#             status=405,
#         )

#     try:
#         topic = Topic.objects.get(id=topic_id)

#         data = json.loads(request.body.decode("utf-8"))
#         new_partition_count = data.get("new_partition_count")

#         if not isinstance(new_partition_count, int) or new_partition_count < 1:
#             try:
#                 new_partition_count = int(new_partition_count)
#                 if new_partition_count < 1:
#                     raise ValueError
#             except Exception:
#                 return JsonResponse(
#                     {"success": False, "message": "Invalid new_partition_count"},
#                     status=400,
#                 )

            
            
#             # return JsonResponse(
#             #     {"success": False, "message": "Invalid new_partition_count"},
#             #     status=400,
#             # )

#         admin_client = AdminClient(KAFKA_CONF)
#         metadata = admin_client.list_topics(timeout=10)

#         if topic.name not in metadata.topics:
#             return JsonResponse(
#                 {"success": False, "message": "Topic not found in Kafka"},
#                 status=400,
#             )

#         current_partitions = len(metadata.topics[topic.name].partitions)

#         if new_partition_count <= current_partitions:
#             return JsonResponse(
#                 {
#                     "success": False,
#                     "message": f"Cannot reduce partitions. Current: {current_partitions}",
#                 },
#                 status=400,
#             )

#         # ✅ CORRECT USAGE
#         fs = admin_client.create_partitions([
#             NewPartitions(topic.name, new_total_count=new_partition_count)
#         ])

#         for _, f in fs.items():
#             f.result()

#         topic.partitions = new_partition_count
#         topic.save()

#         return JsonResponse({
#             "success": True,
#             "message": f"Partitions increased {current_partitions} → {new_partition_count}"
#         })

#     except Topic.DoesNotExist:
#         return JsonResponse({"success": False, "message": "Topic not found"}, status=404)
#     except Exception as e:
#         logger.error(str(e))
#         return JsonResponse({"success": False, "message": str(e)}, status=500)

@csrf_exempt
def alter_topic_partitions(request, topic_id):
    if request.method != "PATCH":
        return JsonResponse({"success": False, "message": "Invalid request method. Use PATCH."})

    try:
        topic = Topic.objects.get(id=topic_id)
        data = json.loads(request.body.decode("utf-8"))
        new_partition_count = data.get("new_partition_count")

        # Ensure valid partition count
        if not new_partition_count or not isinstance(new_partition_count, int):
            try:
                new_partition_count = int(new_partition_count)
            except ValueError:
                return JsonResponse(
                    {"success": False, "message": "Invalid partition count"},
                    status=400,
                )

        if new_partition_count < 1:
            return JsonResponse({"success": False, "message": "Partitions must be greater than 0"}, status=400)

        admin_client = AdminClient(KAFKA_CONF)
        metadata = admin_client.list_topics(timeout=10)

        if topic.name not in metadata.topics:
            return JsonResponse(
                {"success": False, "message": f"Topic '{topic.name}' not found in Kafka cluster."},
                status=400,
            )

        current_partitions = len(metadata.topics[topic.name].partitions)

        if new_partition_count <= current_partitions:
            return JsonResponse(
                {"success": False, "message": f"Cannot reduce partitions. Current: {current_partitions}, Requested: {new_partition_count}"},
                status=400,
            )

        fs = admin_client.create_partitions([NewPartitions(topic.name, new_total_count=new_partition_count)])

        for t, f in fs.items():
            f.result()

        topic.partitions = new_partition_count
        topic.save()

        return JsonResponse({
            "success": True,
            "message": f"Partitions for topic '{topic.name}' increased from {current_partitions} → {new_partition_count}"
        })

    except Topic.DoesNotExist:
        return JsonResponse({"success": False, "message": "Topic not found."})
    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
        return JsonResponse({"success": False, "message": str(e)})


@csrf_exempt
@login_required
def request_alter_topic(request, topic_id):
    """
    User sends ALTER request to admin
    """
    if request.method != "POST":
        return JsonResponse(
            {"success": False, "message": "Invalid request method"},
            status=405
        )

    try:
        # 🔹 Fetch topic
        topic = Topic.objects.get(id=topic_id)

        data = json.loads(request.body or "{}")

        new_partitions = data.get("new_partitions")

        # 🔹 Validation
        if new_partitions is None:
            return JsonResponse(
                {"success": False, "message": "new_partitions is required"},
                status=400
            )

        try:
            new_partitions = int(new_partitions)
        except (TypeError, ValueError):
            return JsonResponse(
                {"success": False, "message": "new_partitions must be a number"},
                status=400
            )

        if new_partitions < 1:
            return JsonResponse(
                {"success": False, "message": "new_partitions must be >= 1"},
                status=400
            )

        # Prevent duplicate pending ALTER request
        if TopicRequest.objects.filter(
            topic=topic,
            request_type="ALTER",
            status="PENDING"
        ).exists():
            return JsonResponse(
                {
                    "success": False,
                    "message": "An ALTER request for this topic is already pending"
                },
                status=400
            )

        #  Create ALTER request
        TopicRequest.objects.create(
            topic=topic,
            topic_name=topic.name,
            requested_by=request.user,
            request_type="ALTER",
            new_partitions=new_partitions,
            status="PENDING",
        )

        return JsonResponse(
            {"success": True, "message": "Alter topic request sent to admin"}
        )

    except Topic.DoesNotExist:
        return JsonResponse(
            {"success": False, "message": "Topic not found"},
            status=404
        )

    except json.JSONDecodeError:
        return JsonResponse(
            {"success": False, "message": "Invalid JSON payload"},
            status=400
        )

    except Exception as e:
        return JsonResponse(
            {"success": False, "message": str(e)},
            status=500
        )

# @csrf_exempt
# @login_required
# def request_alter_topic(request, topic_id):
#     """
#     User sends ALTER request to admin
#     """
#     if request.method != "POST":
#         return JsonResponse(
#             {"success": False, "message": "Invalid request method"},
#             status=405
#         )

#     try:
#         topic = Topic.objects.get(id=topic_id)

#         #  Internal topics cannot be altered
#         if topic.is_internal:
#             return JsonResponse({
#                 "success": False,
#                 "message": "Internal topics cannot be altered"
#             }, status=400)

#         data = json.loads(request.body.decode("utf-8"))

#         new_partitions = data.get("new_partitions")
#         new_replication_factor = data.get("new_replication_factor")  # optional

#         if not new_partitions:
#             return JsonResponse({
#                 "success": False,
#                 "message": "new_partitions is required"
#             }, status=400)

#         # Create ALTER request
#         TopicRequest.objects.create(
#             topic=topic,
#             topic_name=topic.name,
#             requested_by=request.user,
#             request_type="ALTER",
#             new_partitions=new_partitions,
#             status="PENDING",
#             # new_replication_factor=new_replication_factor,
#         )

#         return JsonResponse({
#             "success": True,
#             "message": "Alter topic request sent to admin"
#         })

#     except Topic.DoesNotExist:
#         return JsonResponse({
#             "success": False,
#             "message": "Topic not found"
#         }, status=404)

#     except Exception as e:
#         return JsonResponse({
#             "success": False,
#             "message": str(e)
#         }, status=500)



# @csrf_exempt
# def alter_topic_partitions(request,topic_id):
#     if request.method == "POST":
#         try:
#             data = json.loads(request.body)
#             topic_name = data.get(id=topic_id)
#             new_partitions = int(data.get("partitions"))

#             # Connect to Kafka cluster
#             # admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
#             admin_client = AdminClient(KAFKA_CONF)

#             # Alter topic partitions
#             admin_client.create_partitions(
#                 [NewPartitions(topic_name, new_partitions)]
#             )

#             # Update in Django DB
#             Topic.objects.filter(name=topic_name).update(partitions=new_partitions)

#             return JsonResponse({"success": True, "message": "Topic updated successfully!"})
#         except Exception as e:
#             return JsonResponse({"success": False, "message": str(e)})

#     return JsonResponse({"success": False, "message": "Invalid request method"})


# delete topic

@login_required
def delete_topic(request):
    if request.method == "POST":
        topic_ids = request.POST.getlist("topic_ids")
        if not topic_ids:
            messages.error(request, "No topics selected for deletion.")
            return render(request, "create_topic.html", {
                "username": request.user.username,
                "topics": Topic.objects.filter(created_by=request.user, is_active=True)
            })
        try:
            # admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
            admin_client = AdminClient(KAFKA_CONF)
            for topic_id in topic_ids:
                topic = Topic.objects.get(id=topic_id, created_by=request.user, is_active=True)
                try:
                    admin_client.delete_topics([topic.name])
                    logger.info(f"Kafka topic '{topic.name}' deleted by {request.user.username}")
                except Exception as e:
                    logger.error(f"Kafka topic deletion failed: {e}")
                    messages.error(request, f"Failed to delete topic '{topic.name}' from Kafka: {str(e)}")
                    continue
                topic.delete()  # Permanent deletion
                logger.info(f"Topic '{topic.name}' permanently deleted in Django by {request.user.username}")
            messages.success(request, "Selected topics deleted successfully!")
            return redirect("home" if not request.user.is_superuser else "admin_dashboard")
        except Topic.DoesNotExist:
            messages.error(request, "One or more selected topics not found or you don't have permission.")
            return render(request, "create_topic.html", {
                "username": request.user.username,
                "topics": Topic.objects.filter(created_by=request.user, is_active=True)
            })
    return JsonResponse({"success": False, "message": "Invalid request"})

@login_required
def topic_detail(request, topic_name):
    try:
        topic = Topic.objects.get(name=topic_name, is_active=True, created_by=request.user)
        topic_request = TopicRequest.objects.filter(
            topic_name=topic_name,
            requested_by=request.user,
            status='APPROVED'
        ).order_by('-reviewed_at').first()
        request_id = topic_request.id if topic_request else None
        context = {
            "topic": topic,
            "username": request.user.username,
            "topics": Topic.objects.filter(created_by=request.user, is_active=True),
            "request_id": request_id
        }
        return render(request, "topic_detail.html", context)
    except Topic.DoesNotExist:
        return render(request, "topic_detail.html", {
            "topics": Topic.objects.filter(is_active=True, created_by=request.user),
            "username": request.user.username,
            "error": f"Topic {topic_name} does not exist or you don't have permission."
        })

def execute_confluent_command(command, topic_name=None, partitions=None):
    return False, "This is a placeholder function for confluent command"


# @csrf_exempt
# def topic_detail_api(request, topic_name):
#     try:
#         topic = Topic.objects.get(name=topic_name, is_active=True)
#         data = {
#             "id": topic.id,
#             "name": topic.name,
#             "partitions": topic.partitions,
#             "replication":topic.replications,
#             "created_by": topic.created_by.username,
#             "production": topic.production,
#             "consumption": topic.consumption,
#             "followers": topic.followers,
#             "observers": topic.observers,
#             "last_produced": topic.last_produced,
#             # "created_at": topic.created_at.strftime("%d/%m/%Y, %H:%M:%S"),
#             "created_at": timezone.localtime(topic.created_at).strftime("%d/%m/%Y, %H:%M:%S"),
            
#         }
#         return JsonResponse({"success": True, "topic": data})
#     except Topic.DoesNotExist:
#         return JsonResponse({"success": False, "message": "Topic not found"}, status=404)

# @csrf_exempt
# def topic_detail_api(request, topic_name):
#     if request.session.get("role") not in ["admin", "user"]:
#         return JsonResponse({"success": False, "message": "Access denied"}, status=403)

#     try:
#         # 1️ Try DB topic first (APP-CREATED)
#         topic = Topic.objects.filter(name=topic_name).first()

#         # 2 Always fetch Kafka metadata (SOURCE OF TRUTH FOR VIEW)
#         admin_client = AdminClient(KAFKA_CONF)
#         md = admin_client.list_topics(timeout=10)
#         kafka_topic = md.topics.get(topic_name)

#         if not kafka_topic:
#             return JsonResponse(
#                 {"success": False, "message": "Topic not found"},
#                 status=404
#             )

#         # 3️ DB topic exists → NON-INTERNAL
#         if topic:
#             data = {
#                 "id": topic.id,
#                 "name": topic.name,
#                 "partitions": topic.partitions,
#                 "replication": topic.replications,
#                 "created_by": topic.created_by.username,
#                 "production": topic.production,
#                 "consumption": topic.consumption,
#                 "followers": topic.followers,
#                 "observers": topic.observers,
#                 "last_produced": topic.last_produced,
#                 "created_at": timezone.localtime(topic.created_at).strftime(
#                     "%d/%m/%Y, %H:%M:%S"
#                 ),
#                 "is_internal": False,
#             }

#         # 4️Kafka-only topic → INTERNAL
#         else:
#             partitions = len(kafka_topic.partitions)
#             replication = (
#                 len(list(kafka_topic.partitions.values())[0].replicas)
#                 if kafka_topic.partitions else 0
#             )

#             data = {
#                 "id": None,
#                 "name": topic_name,
#                 "partitions": partitions,
#                 "replication": replication,
#                 "created_by": "CLI / System",
#                 "production": "N/A",
#                 "consumption": "N/A",
#                 "followers": 0,
#                 "observers": 0,
#                 "last_produced": "N/A",
#                 "created_at": "N/A",
#                 "is_internal": True,
#             }

#         return JsonResponse({"success": True, "topic": data})

#     except Exception as e:
#         return JsonResponse(
#             {"success": False, "message": str(e)},
#             status=500
#         )


@csrf_exempt
def topic_detail_api(request, topic_name):
    # Role check
    if request.session.get("role") not in ["admin", "user"]:
        return JsonResponse(
            {"success": False, "message": "Access denied"},
            status=403
        )

    try:
        # 1️ Fetch DB topic (APP-created topics)
        topic = Topic.objects.filter(name=topic_name).first()

        # 2️ Try fetching Kafka metadata (DO NOT hard-fail)
        kafka_topic = None
        try:
            admin_client = AdminClient(KAFKA_CONF)
            md = admin_client.list_topics(timeout=10)
            kafka_topic = md.topics.get(topic_name)
        except KafkaException:
            # Kafka down / metadata lag — allowed for DB topics
            kafka_topic = None

        # 3️ If neither DB nor Kafka has the topic → real 404
        if not topic and not kafka_topic:
            return JsonResponse(
                {"success": False, "message": "Topic not found"},
                status=404
            )

        # 4️ DB-backed topic → NON-INTERNAL (PRIMARY CASE)
        if topic:
            data = {
                "id": topic.id,
                "name": topic.name,
                "partitions": topic.partitions,
                "replication": topic.replications,
                "created_by": topic.created_by.username if topic.created_by else "Unknown",
                "production": topic.production,
                "consumption": topic.consumption,
                "followers": topic.followers,
                "observers": topic.observers,
                "last_produced": topic.last_produced,
                "created_at": timezone.localtime(topic.created_at).strftime(
                    "%d/%m/%Y, %H:%M:%S"
                ),
                "is_internal": False,
            }

        # 5️ Kafka-only topic → INTERNAL
        else:
            partitions = len(kafka_topic.partitions)
            replication = (
                len(next(iter(kafka_topic.partitions.values())).replicas)
                if kafka_topic.partitions else 0
            )

            data = {
                "id": None,
                "name": topic_name,
                "partitions": partitions,
                "replication": replication,
                "created_by": "Kafka / System",
                "production": "N/A",
                "consumption": "N/A",
                "followers": 0,
                "observers": 0,
                "last_produced": "N/A",
                "created_at": "N/A",
                "is_internal": True,
            }

        return JsonResponse({"success": True, "topic": data})

    except Exception as e:
        return JsonResponse(
            {"success": False, "message": str(e)},
            status=500
        )



# @login_required
# def create_partition(request, topic_name):
#     if not request.user.is_authenticated:
#         return redirect("login")
#     if request.method == "POST":
#         partitions_to_delete = request.POST.get("partitions")
#         if partitions_to_delete:
#             try:
#                 partitions_to_delete = int(partitions_to_delete)
#                 if partitions_to_delete < 1:
#                     return render(request, "topic_detail.html", {
#                         "topic": Topic.objects.get(name=topic_name, is_active=True, created_by=request.user),
#                         "username": request.user.username,
#                         "topics": Topic.objects.filter(created_by=request.user, is_active=True),
#                         "error": "Partitions must be at least 1."
#                     })
#                 topic = Topic.objects.get(name=topic_name, is_active=True, created_by=request.user)
#                 if partitions_to_delete >= topic.partitions:
#                     return render(request, "topic_detail.html", {
#                         "topic": topic,
#                         "username": request.user.username,
#                         "topics": Topic.objects.filter(created_by=request.user, is_active=True),
#                         "error": "Cannot delete more partitions than exist."
#                     })
#                 if topic.created_by != request.user and not request.user.is_superuser:
#                     return render(request, "topic_detail.html", {
#                         "topic": topic,
#                         "username": request.user.username,
#                         "topics": Topic.objects.filter(created_by=request.user, is_active=True),
#                         "error": "You can only delete partitions from topics you created."
#                     })
#                 success, message = execute_confluent_command("delete_partition", topic_name, partitions_to_delete)
#                 if (success):
#                     topic.partitions -= partitions_to_delete
#                     if topic.partitions <= 0:
#                         topic.delete()  # Permanently delete topic if no partitions remain
#                         logger.info(f"Topic '{topic_name}' permanently deleted due to no partitions by {request.user.username}")
#                     else:
#                         topic.save()
#                         LogEntry.objects.create(
#                             command=f"delete_partition_{topic_name}",
#                             approved=True,
#                             message=f"Permanently deleted {partitions_to_delete} partitions from {topic_name} by {request.user.username}"
#                         )
#                         logger.info(f"Permanently deleted {partitions_to_delete} partitions from {topic_name} by {request.user.username}")
#                     return redirect("admin_dashboard" if request.user.is_superuser else "home")
#                 else:
#                     return render(request, "topic_detail.html", {
#                         "topic": topic,
#                         "username": request.user.username,
#                         "topics": Topic.objects.filter(created_by=request.user, is_active=True),
#                         "error": message
#                     })
#             except ValueError:
#                 return render(request, "topic_detail.html", {
#                     "topic": Topic.objects.get(name=topic_name, is_active=True, created_by=request.user),
#                     "username": request.user.username,
#                     "topics": Topic.objects.filter(created_by=request.user, is_active=True),
#                     "error": "Invalid number of partitions."
#                 })
#             except Topic.DoesNotExist:
#                 return render(request, "topic_detail.html", {
#                     "topics": Topic.objects.filter(is_active=True, created_by=request.user),
#                     "username": request.user.username,
#                     "error": f"Topic {topic_name} does not exist or you don't have permission."
#                 })
#         return render(request, "topic_detail.html", {
#             "topic": Topic.objects.get(name=topic_name, is_active=True, created_by=request.user),
#             "username": request.user.username,
#             "topics": Topic.objects.filter(created_by=request.user, is_active=True),
#             "error": "Please specify the number of partitions."
#         })
#     return redirect("home" if not request.user.is_superuser else "admin_dashboard")

@login_required
def delete_partition(request, topic_name):
    # Redirect to topic_detail for consistency, where deletion is handled
    return redirect("topic_detail", topic_name=topic_name)

# @login_required
# def submit_request(request):
#     if request.method == "POST":
#         topic_name = request.POST.get("topic_name")
#         partitions = request.POST.get("partitions")
#         if topic_name and partitions:
#             try:
#                 partitions = int(partitions)
#                 if partitions < 1:
#                     return JsonResponse({"success": False, "message": "Partitions must be at least 1."})
#                 TopicRequest.objects.create(
#                     topic_name=topic_name,
#                     partitions=partitions,
#                     requested_by=request.user
#                 )
#                 logger.info(f"Topic request for {topic_name} submitted by {request.user.username}")
#                 return JsonResponse({"success": True, "message": "Topic creation request submitted"})
#             except ValueError:
#                 return JsonResponse({"success": False, "message": "Invalid number of partitions."})
#         return JsonResponse({"success": False, "message": "Please fill all fields."})
#     return JsonResponse({"success": False, "message": "Invalid request"})


# @csrf_exempt
# @login_required
# def approve_request(request, request_id):
#     if not request.user.is_authenticated or not request.user.is_superuser:
#         return JsonResponse({"success": False, "message": "Unauthorized"}, status=403)

#     if request.method == "POST":
#         try:
#             topic_request = TopicRequest.objects.get(id=request_id, status='PENDING')
#             topic_request.status = 'APPROVED'
#             topic_request.reviewed_by = request.user
#             topic_request.reviewed_at = timezone.now()
#             topic_request.save()

#             logger.info(f"Request {request_id} approved by {request.user.username}")

#             return JsonResponse({
#                 "success": True,
#                 "message": f"Request for '{topic_request.topic_name}' approved!",
#                 "topic": {
#                     "id": topic_request.id,
#                     "topic_name": topic_request.topic_name,
#                     "partitions": topic_request.partitions,
#                     "status": topic_request.status,
#                     "requested_by": topic_request.requested_by.username,
#                 },
#             })
#         except TopicRequest.DoesNotExist:
#             return JsonResponse({"success": False, "message": "Request not found or already processed."}, status=404)

#     return JsonResponse({"success": False, "message": "Invalid request method."}, status=405)




# @csrf_exempt
# @login_required
# def approve_request(request, request_id):
#     if not request.user.is_authenticated or not request.user.is_superuser:
#         return JsonResponse({"success": False, "message": "Unauthorized"}, status=403)

#     if request.method == "POST":
#         try:
#             topic_request = TopicRequest.objects.get(id=request_id, status='PENDING')
#             request_type = topic_request.request_type  # "creation" or "deletion"

#             # Kafka admin client setup
#             admin_client = AdminClient(KAFKA_CONF)

#             if request_type == "CREATE":
#                 #  Create Topic in Kafka
#                 new_topic = NewTopic(
#                     topic_request.topic_name,
#                     num_partitions=topic_request.partitions or 1,
#                     replication_factor=1
#                 )
#                 admin_client.create_topics([new_topic])

#                 #  Save to DB
#                 Topic.objects.create(
#                     name=topic_request.topic_name,
#                     partitions=topic_request.partitions,
#                     created_by=topic_request.requested_by
#                 )

#                 message = f"Topic '{topic_request.topic_name}' created successfully."

#             elif request_type == "DELETE":
#                 # Delete Topic in Kafka
#                 try:
#                     admin_client.delete_topics([topic_request.topic_name])
#                 except Exception as e:
#                     logger.error(f"Kafka deletion failed: {str(e)}")
#                     return JsonResponse({"success": False, "message": f"Kafka deletion failed: {str(e)}"}, status=500)

#                 # Remove from DB
#                 Topic.objects.filter(name=topic_request.topic_name).delete()
#                 message = f"Topic '{topic_request.topic_name}' deleted successfully."
                
#             elif request_type == "ALTER":
                
#                 topic = Topic.objects.get(name=topic_request.topic_name)

#                 # if not topic_request.new_partitions:
#                 #     return JsonResponse(
#                 #         {"success": False, "message": "Missing new_partitions"},
#                 #         status=400,
#                 #     )
                
#                 if topic_request.new_partitions is None:
#                     return JsonResponse(
#                         {"success": False, "message": "Invalid alter request (no partitions)"},
#                         status=400,
#                     )


#                 admin_client = AdminClient(KAFKA_CONF)
#                 metadata = admin_client.list_topics(timeout=10)

#                 if topic.name not in metadata.topics:
#                     return JsonResponse(
#                         {"success": False, "message": "Topic not found in Kafka"},
#                         status=400,
#                     )

#                 current_partitions = len(metadata.topics[topic.name].partitions)
#                 new_partitions = topic_request.new_partitions

#                 if new_partitions <= current_partitions:
#                     return JsonResponse(
#                         {
#                             "success": False,
#                             "message": f"Cannot reduce partitions. Current: {current_partitions}",
#                         },
#                         status=400,
#                     )

#                 # ✅ CORRECT
#                 fs = admin_client.create_partitions([
#                     NewPartitions(topic.name, new_total_count=new_partitions)
#                 ])

#                 for _, f in fs.items():
#                     f.result()

#                 topic.partitions = new_partitions
#                 topic.save()

#                 message = f"Topic '{topic.name}' altered ({current_partitions} → {new_partitions})"

            
#             # elif request_type == "ALTER":
#             #     topic = Topic.objects.get(name=topic_request.topic_name)

#             #     metadata = admin_client.list_topics(timeout=10)
#             #     current_partitions = len(
#             #         metadata.topics[topic.name].partitions
#             #     )

#             #     new_partitions = topic_request.new_partitions

#             #     if new_partitions <= current_partitions:
#             #         return JsonResponse({
#             #             "success": False,
#             #             "message": f"Cannot reduce partitions. Current: {current_partitions}"
#             #         }, status=400)

#             #     fs = admin_client.create_partitions([
#             #         NewPartitions(topic.name, new_total_count=new_partitions)
#             #     ])

#             #     for _, f in fs.items():
#             #         f.result()

#             #     # Update DB
#             #     topic.partitions = new_partitions
#             #     topic.save()

#             #     message = (
#             #         f"Topic '{topic.name}' altered "
#             #         f"({current_partitions} → {new_partitions})"
#             #     )    

#             else:
#                 return JsonResponse({"success": False, "message": "Invalid request type."}, status=400)

#             # Update Request Status
#             topic_request.status = 'APPROVED'
#             topic_request.reviewed_by = request.user
#             topic_request.reviewed_at = timezone.now()
#             topic_request.save()

#             logger.info(f"Request {request_id} ({request_type}) approved by {request.user.username}")

#             return JsonResponse({
#                 "success": True,
#                 "message": message,
#                 "topic": {
#                     "id": topic_request.id,
#                     "topic_name": topic_request.topic_name,
#                     "partitions": topic_request.partitions,
#                     "status": topic_request.status,
#                     "requested_by": topic_request.requested_by.username,
#                     "request_type": topic_request.request_type,
#                 },
#             })

#         except TopicRequest.DoesNotExist:
#             return JsonResponse({"success": False, "message": "Request not found or already processed."}, status=404)
#         except Exception as e:
#             logger.error(f"Error approving request {request_id}: {str(e)}")
#             return JsonResponse({"success": False, "message": str(e)}, status=500)

#     return JsonResponse({"success": False, "message": "Invalid request method."}, status=405)


# @csrf_exempt
# @login_required
# def approve_request(request, request_id):
#     if not request.user.is_authenticated or not request.user.is_superuser:
#         return JsonResponse({"success": False, "message": "Unauthorized"}, status=403)

#     if request.method == "POST":
#         try:
#             topic_request = TopicRequest.objects.get(id=request_id, status='PENDING')
            
#             # Check for missing partitions in ALTER request
#             if topic_request.request_type == "ALTER" and topic_request.new_partitions is None:
#                 return JsonResponse(
#                     {"success": False, "message": "Missing new_partitions for ALTER request"},
#                     status=400,
#                 )

#             # Kafka admin client setup
#             admin_client = AdminClient(KAFKA_CONF)

#             # Request type handling
#             if topic_request.request_type == "CREATE":
#                 # Handle topic creation (same as before)
#                 new_topic = NewTopic(
#                     topic_request.topic_name,
#                     num_partitions=topic_request.new_partitions or 1,
#                     replication_factor=1
#                 )
#                 admin_client.create_topics([new_topic])

#                 Topic.objects.create(
#                     name=topic_request.topic_name,
#                     partitions=topic_request.new_partitions,
#                     created_by=topic_request.requested_by
#                 )
#                 message = f"Topic '{topic_request.topic_name}' created successfully."

#             elif topic_request.request_type == "DELETE":
#                 # Handle topic deletion (same as before)
#                 admin_client.delete_topics([topic_request.topic_name])
#                 Topic.objects.filter(name=topic_request.topic_name).delete()
#                 message = f"Topic '{topic_request.topic_name}' deleted successfully."
            
#             elif topic_request.request_type == "ALTER":
#                 topic = Topic.objects.get(name=topic_request.topic_name)
                
#                 if topic.name not in metadata.topics:
#                     return JsonResponse({
#                         "success": False,
#                         "message": f"Topic '{topic.name}' not found in Kafka metadata"
#                     }, status=400)
                                
#                 metadata = admin_client.list_topics(timeout=10)
#                 current_partitions = len(metadata.topics[topic.name].partitions)

#                 new_partitions = topic_request.new_partitions
#                 if new_partitions <= current_partitions:
#                     return JsonResponse(
#                         {"success": False, "message": f"Cannot reduce partitions. Current: {current_partitions}, Requested: {new_partitions}"},
#                         status=400,
#                     )

#                 fs = admin_client.create_partitions([NewPartitions(topic.name, new_total_count=new_partitions)])
#                 for t, f in fs.items():
#                     f.result()

#                 topic.partitions = new_partitions
#                 topic.save()

#                 message = f"Topic '{topic.name}' altered ({current_partitions} → {new_partitions})"
#             else:
#                 return JsonResponse({"success": False, "message": "Invalid request type."}, status=400)

#             # Update Request Status
#             topic_request.status = 'APPROVED'
#             topic_request.reviewed_by = request.user
#             topic_request.reviewed_at = timezone.now()
#             topic_request.save()

#             return JsonResponse({
#                 "success": True,
#                 "message": message,
#                 "topic": {
#                     "id": topic_request.id,
#                     "topic_name": topic_request.topic_name,
#                     "partitions": topic_request.new_partitions,
#                     "status": topic_request.status,
#                     "requested_by": topic_request.requested_by.username,
#                     "request_type": topic_request.request_type,
#                 },
#             })

#         except TopicRequest.DoesNotExist:
#             return JsonResponse({"success": False, "message": "Request not found or already processed."}, status=404)
#         except Exception as e:
#             logger.error(f"Error approving request {request_id}: {str(e)}")
#             return JsonResponse({"success": False, "message": str(e)}, status=500)

#     return JsonResponse({"success": False, "message": "Invalid request method."}, status=405)



@csrf_exempt
@login_required
def approve_request(request, request_id):
    if not request.user.is_superuser:
        return JsonResponse({"success": False, "message": "Unauthorized"}, status=403)

    if request.method != "POST":
        return JsonResponse({"success": False, "message": "Invalid method"}, status=405)

    try:
        tr = TopicRequest.objects.get(id=request_id, status="PENDING")
        admin_client = AdminClient(KAFKA_CONF)
        
        # ---------------- CREATE ----------------
        if tr.request_type == "CREATE":
            try:
                new_topic = NewTopic(
                    tr.topic_name,
                    num_partitions=tr.new_partitions or 1,
                    replication_factor=1
                )

                fs = admin_client.create_topics([new_topic])

                #  WAIT for Kafka to actually create the topic
                for _, f in fs.items():
                    f.result()

            except Exception as e:
                return JsonResponse(
                    {"success": False, "message": f"Kafka create failed: {str(e)}"},
                    status=500
                )

            #  Save to DB ONLY after Kafka success
            Topic.objects.create(
                name=tr.topic_name,
                partitions=tr.new_partitions or 1,
                created_by=tr.requested_by
            )

            message = f"Topic '{tr.topic_name}' created"


        # ---------------- DELETE ----------------
        elif tr.request_type == "DELETE":
            # admin_client.delete_topics([tr.topic_name])
            # Topic.objects.filter(name=tr.topic_name).delete()
            fs = admin_client.delete_topics([tr.topic_name])
            for _, f in fs.items():
                f.result()

            Topic.objects.filter(name=tr.topic_name).delete()

            message = f"Topic '{tr.topic_name}' deleted"

        # ---------------- ALTER ----------------
        elif tr.request_type == "ALTER":
            admin_client = AdminClient(KAFKA_CONF)

            try:
                topic = Topic.objects.get(name=tr.topic_name)
            except Topic.DoesNotExist:
                return JsonResponse(
                    {"success": False, "message": "Topic not found in database"},
                    status=400
                )

            
            try:
                new_partitions = int(tr.new_partitions)
            except (TypeError, ValueError):
                return JsonResponse(
                    {"success": False, "message": "Invalid new partition count"},
                    status=400
                )

            if new_partitions < 1:
                return JsonResponse(
                    {"success": False, "message": "Partitions must be at least 1"},
                    status=400
                )

            metadata = admin_client.list_topics(timeout=10)

            if topic.name not in metadata.topics:
                return JsonResponse(
                    {"success": False, "message": "Topic not found in Kafka"},
                    status=400
                )

            current_partitions = len(metadata.topics[topic.name].partitions)

            if new_partitions <= current_partitions:
                return JsonResponse(
                    {
                        "success": False,
                        "message": f"Partitions can only be increased (current: {current_partitions})"
                    },
                    status=400
                )

            try:
                fs = admin_client.create_partitions([
                    NewPartitions(topic.name, new_partitions)
                ])

                for _, f in fs.items():
                    f.result()   # wait for Kafka

            except Exception as e:
                return JsonResponse(
                    {"success": False, "message": f"Kafka alter failed: {str(e)}"},
                    status=500
                )

            topic.partitions = new_partitions
            topic.save()

            message = (
                f"Topic '{topic.name}' partitions updated "
                f"({current_partitions} → {new_partitions})"
            )

        else:
            return JsonResponse(
                {"success": False, "message": "Invalid request type"},
                status=400
            )

        tr.status = "APPROVED"
        tr.save()

        # return JsonResponse({"success": True, "message": message})
        return JsonResponse({
            "success": True,
            "message": message,
            "request_id": tr.id,
            "status": tr.status,
            "topic_name": tr.topic_name,
            "request_type": tr.request_type,
        })

    except TopicRequest.DoesNotExist:
        return JsonResponse(
            {"success": False, "message": "Request not found"},
            status=404
        )
    except Exception as e:
        return JsonResponse(
            {"success": False, "message": str(e)},
            status=500
        )


@csrf_exempt
@login_required
def decline_request(request, request_id):
    if not request.user.is_authenticated or not request.user.is_superuser:
        return JsonResponse({"success": False, "message": "Unauthorized"}, status=403)

    if request.method == "POST":
        try:
            data = json.loads(request.body or "{}")
            decline_reason = data.get("decline_reason")

            if not decline_reason:
                return JsonResponse(
                    {"success": False, "message": "Decline reason is required"},
                    status=400
                )
            
            topic_request = TopicRequest.objects.get(id=request_id, status='PENDING')
            topic_request.status = 'DECLINED'
            topic_request.decline_reason = decline_reason
            topic_request.reviewed_by = request.user
            topic_request.reviewed_at = timezone.now()
            topic_request.save()

            logger.info(f"Request {request_id} declined by {request.user.username}")

            # return JsonResponse({
            #     "success": True,
            #     "message": f"Request for '{topic_request.topic_name}' declined."
            # })
            
            return JsonResponse({
                "success": True,
                "message": f"Request for '{topic_request.topic_name}' declined.",
                "request_id": topic_request.id,
                "status": "DECLINED",
                "topic_name": topic_request.topic_name,
                "request_type": topic_request.request_type,
            })

        except TopicRequest.DoesNotExist:
            return JsonResponse({"success": False, "message": "Request not found or already processed."}, status=404)

    return JsonResponse({"success": False, "message": "Invalid request method."}, status=405)


# @csrf_exempt
# @login_required
# def topic_request(request,request_id=None):
#     if request.method == "POST":
#         data = json.loads(request.body)
#         topic_name = data.get("topic_name")
#         # partitions = data.get("partitions", 1)
#         request_type = data.get("request_type", "creation")
        
#         new_partitions = data.get("new_partitions")
#         new_replication_factor = data.get("new_replication_factor")
        
#         if not topic_name or not request_type:
#             return JsonResponse(
#                 {"success": False, "message": "Missing required fields"},
#                 status=400
#             )

#         TopicRequest.objects.create(
#             topic_name=topic_name,
#             requested_by=request.user,
#             # purpose=request_type,
#             request_type=request_type.upper(),
#             new_partitions=new_partitions if request_type == "ALTER" else None,
#             new_replication_factor=new_replication_factor,
#             status="PENDING"
#         )
#         return JsonResponse({"success": True, "message": "Request sent to admin."})

#     elif request.method == "DELETE":
#         # --- Delete a pending request ---
#         try:
#             topic_request = TopicRequest.objects.get(id=request_id, requested_by=request.user, status="PENDING")
#             topic_request.delete()
#             return JsonResponse({"success": True, "message": "Request deleted successfully."})
#         except TopicRequest.DoesNotExist:
#             return JsonResponse({"success": False, "message": "Request not found or cannot be deleted."}, status=404)

#     return JsonResponse({"success": False, "message": "Invalid request method."}, status=405)

@csrf_exempt
@login_required
def topic_request(request, request_id=None):
    
    if request.method == "POST":
        try:
            data = json.loads(request.body)

            topic_name = data.get("topic_name")
            request_type = data.get("request_type", "").upper()

            # Allow ONLY CREATE & DELETE here
            if request_type not in ["CREATE", "DELETE"]:
                return JsonResponse(
                    {"success": False, "message": "Invalid request type"},
                    status=400
                )

            if not topic_name:
                return JsonResponse(
                    {"success": False, "message": "Topic name is required"},
                    status=400
                )

            # CREATE request
            if request_type == "CREATE":
                new_partitions = data.get("new_partitions")

                if not new_partitions or int(new_partitions) < 1:
                    return JsonResponse(
                        {"success": False, "message": "Valid new_partitions is required"},
                        status=400
                    )

                TopicRequest.objects.create(
                    topic_name=topic_name,
                    requested_by=request.user,
                    request_type="CREATE",
                    new_partitions=int(new_partitions),
                    status="PENDING",
                )

                return JsonResponse(
                    {"success": True, "message": "Topic creation request sent to admin"}
                )
            

            # DELETE request
            if request_type == "DELETE":
                
                # Restrict duplicate pending delete requests
                existing_request = TopicRequest.objects.filter(
                    topic_name=topic_name,
                    request_type="DELETE",
                    requested_by=request.user,
                    status="PENDING"
                ).exists()

                if existing_request:
                    return JsonResponse(
                        {
                            "success": False,
                            "message": "Deletion request already exists for this topic"
                        },
                        status=400
                    )
                
                topic = Topic.objects.filter(name=topic_name).first()
                partitions = topic.partitions if topic else None
                
                TopicRequest.objects.create(
                    topic_name=topic_name,
                    requested_by=request.user,
                    request_type="DELETE",
                    new_partitions=partitions,
                    status="PENDING",
                )
                return JsonResponse(
                    {"success": True, "message": "Topic deletion request sent to admin"}
                )

        except json.JSONDecodeError:
            return JsonResponse(
                {"success": False, "message": "Invalid JSON payload"},
                status=400
            )

        except Exception as e:
            return JsonResponse(
                {"success": False, "message": str(e)},
                status=500
            )

    # DELETE PENDING REQUEST (CANCEL) 
    elif request.method == "DELETE":
        try:
            tr = TopicRequest.objects.get(
                id=request_id,
                requested_by=request.user,
                status="PENDING"
            )
            tr.delete()
            return JsonResponse(
                {"success": True, "message": "Request deleted successfully"}
            )

        except TopicRequest.DoesNotExist:
            return JsonResponse(
                {"success": False, "message": "Request not found or cannot be deleted"},
                status=404
            )

    return JsonResponse(
        {"success": False, "message": "Invalid request method"},
        status=405
    )

from django.views.decorators.http import require_GET
from confluent_kafka import KafkaException

@require_GET
def kafka_cluster_status(request):
    try:
        admin_client = AdminClient(KAFKA_CONF)

        metadata = admin_client.list_topics(timeout=5)

        brokers = metadata.brokers or {}
        active_brokers = len(brokers)

        #  Kafka does not directly expose "inactive brokers"
        # So we treat:
        # - If metadata fetch succeeds → brokers are active
        # - If fetch fails → cluster inactive
        inactive_brokers = 0

        return JsonResponse({
            "status": "ACTIVE" if active_brokers > 0 else "INACTIVE",
            "active_brokers": active_brokers,
            "inactive_brokers": inactive_brokers,
            "total_brokers": active_brokers + inactive_brokers
        })

    except KafkaException:
        return JsonResponse({
            "status": "INACTIVE",
            "active_brokers": 0,
            "inactive_brokers": 0,
            "total_brokers": 0
        })

    except Exception:
        return JsonResponse({
            "status": "INACTIVE",
            "active_brokers": 0,
            "inactive_brokers": 0,
            "total_brokers": 0
        })


# @require_GET
# def kafka_cluster_status(request):
#     try:
#         admin_client = AdminClient(KAFKA_CONF)

#         # Try fetching metadata (fast health check)
#         metadata = admin_client.list_topics(timeout=5)

#         if metadata.brokers:
#             return JsonResponse({
#                 "status": "ACTIVE"
#             })

#         return JsonResponse({
#             "status": "INACTIVE"
#         })

#     except KafkaException:
#         return JsonResponse({
#             "status": "INACTIVE"
#         })
#     except Exception:
#         return JsonResponse({
#             "status": "INACTIVE"
#         })