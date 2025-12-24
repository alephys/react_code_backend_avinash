#accounts ldap_config.py
from decouple import config

LDAP_SERVER_URL = config('LDAP_SERVER_URL')
GROUP_BASE = config('GROUP_BASE')
USER_BASE = config('USER_BASE')
BIND_DN = config('BIND_DN')
BIND_PASSWORD = config('BIND_PASSWORD')