from main import * # import functions from main

URI = "neo4j://localhost:7687"
# Grab admin secret
secret = getSecret()
# Connect to db
db = Graph(URI, ("neo4j", secret))

db.query(f"MATCH (n) SET n.{db.datestr} = 0")
db.query(f"MATCH ()-[l]-() SET l.{db.datestr} = 0")
