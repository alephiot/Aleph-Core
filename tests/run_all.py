import subprocess


tests = [
    "test_connection.ConnectionTestCase",
    "test_db.RDSGenericTestCase",
    "test_mariadb.MariaDBTestCase",
    "test_mongodb.MongoDBTestCase",
    "test_mongodb.MongoDBTestCase",
]

for test in tests:
    print(subprocess.run(['ls', '-l'], stdout=subprocess.PIPE).stdout)
