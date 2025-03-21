import redshift_connector

conn = redshift_connector.connect(
    host='default-workgroup.207567756516.us-east-1.redshift-serverless.amazonaws.com',
    database='dev',
    port=5439,
    user='admin',
    password='FYZVwhdhy002&!'
)

cursor = conn.cursor()
cursor.execute("select * from test.users")
result = cursor.fetchall()
print(result)