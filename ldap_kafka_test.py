# myproject/ldap_kafka_test.py
from ldap3 import Server, Connection, ALL
from accounts.ldap_config import LDAP_SERVER_URL, GROUP_BASE, USER_BASE, BIND_DN, BIND_PASSWORD

server = Server(LDAP_SERVER_URL, get_info=ALL)
conn = Connection(server, user=BIND_DN, password=BIND_PASSWORD, auto_bind=True)
print("Connected to LDAP successfully!")

conn.search(USER_BASE, "(objectClass=inetOrgPerson)", attributes=['uid', 'cn'])
print("Users found with details:")
for entry in conn.entries:
    print(f"DN: {entry.entry_dn}, uid: {entry.uid}, cn: {entry.cn}")

conn.search(GROUP_BASE, "(objectClass=posixGroup)", attributes=['cn', 'memberUid'])
print("\nGroups found:")
for entry in conn.entries:
    print(f"Group: {entry.cn}, members: {entry.memberUid}")

conn.unbind()