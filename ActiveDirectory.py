import pyad.adquery
q = pyad.adquery.ADQuery()

q.execute_query(
    attributes = ["distinguishedName", "description"],
    where_clause = "objectClass = '*'",
    base_dn = "OU=people, DC=kodak, C=us"
)

for row in q.get_results():
    print row["distinguishedName"]