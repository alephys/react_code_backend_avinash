# myproject auth_backends.py
from ldap3 import Server, Connection, ALL, core
from django.contrib.auth.models import User

class LDAPBackend:
    def authenticate(self, request, username=None, password=None):
        server = Server("ldap://ldapnode.infra.alephys.com:389", get_info=ALL)

        try:
            conn = Connection(
                server,
                # f"uid={username},dc=example,dc=com",
                f"uid={username},cn=users,cn=accounts,dc=alephys,dc=com",
                password,
                auto_bind=True
            )
        except core.exceptions.LDAPException:
            # Could not bind → return None so Django tries next backend
            return None

        if conn.bound:
            # LDAP authentication successful
            try:
                user = User.objects.get(username=username)
            except User.DoesNotExist:
                user = User.objects.create_user(username=username, password=password)
            return user

        return None

    def get_user(self, user_id):
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None
