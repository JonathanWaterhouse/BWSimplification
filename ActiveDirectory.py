import pyad.adquery, pyad
from pyad import aduser
pyad.pyad_setdefaults(ldap_server="directoryko.kodak.com",username="30104675", password = "Il2w@hpie")
user = aduser.ADUser.from_cn("30104675")
#<LDAP://directoryko.kodak.com/ou=People,o=Kodak,c=us>

q = pyad.adquery.ADQuery()

q.execute_query(
    attributes = ["employeenumber", "displayName", "SN", "givenName", "department", "c", "mail"],
    where_clause = "employeenumber = '50327882'",
    #base_dn = "ou=Users,ou=US,dc=ekc1,dc=ekc,dc=kodak,dc=com"
    #base_dn = "ou=Users,ou=GB,dc=ekc2,dc=ekc,dc=kodak,dc=com"
    #base_dn = "ou=Users"
)
"""
cn=U104675,ou=Users,ou=GB,dc=ekc2,dc=ekc,dc=kodak,dc=com on EKC2
employeenumber=30104675,l=GB,ou=People,o=Kodak,c=US on EKDir NetPass
"""

print q.get_row_count()
for row in q.get_results():
    if repr(row["employeenumber"]) != 'None':
        print repr(row["employeenumber"]) + repr(row["displayName"]) + repr(row["SN"])+  \
              repr(row["givenName"]) + repr(row["department"])\
              + repr(row["c"]) + repr(row["mail"])

