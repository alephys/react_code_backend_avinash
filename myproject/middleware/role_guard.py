from django.http import JsonResponse

class RoleGuardMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        role = request.session.get("role")

        # Only user allowed for user API
        if request.path.startswith("/home_api") and role != "user":
            return JsonResponse({"success": False, "message": "Access denied: user only"}, status=403)

        # Only admin allowed for admin API
        if request.path.startswith("/admin_dashboard_api") and role != "admin":
            return JsonResponse({"success": False, "message": "Access denied: admin only"}, status=403)

        return self.get_response(request)
