# Runs on python 3.8 (not 3.9 at time of writing), because of Feedparser's requirements
import os
from datetime import date

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

# load .env
from dotenv import load_dotenv
load_dotenv()

def getSecret():
    return(os.environ["PASSWORD"])

# check connection
URI = "neo4j://localhost:7687"
secret = getSecret()

with GraphDatabase.driver(URI, auth=("neo4j", secret)) as driver:
    driver.verify_connectivity()

# For large queries
def divide_chunks(l, n):
     
    for i in range(0, len(l), n):
        yield l[i:i + n]

# Make interacting with db easier with this class
class Graph:

    def __init__(self, URI: str, AUTH: tuple[str, str]):
        self.driver = GraphDatabase.driver(URI, auth=AUTH)
        self.driver.verify_connectivity()

        self.datestr = f"y{date.today().isocalendar().year}w{date.today().isocalendar().week}"

    def query(self, query: str, verbose = False):
        records, summary, keys = self.driver.execute_query(query)
        if verbose:
            stump = summary.query if len(summary.query) <= 50 else summary.query[0:49] + "..."
            print("The query `{query}`, in {time} ms, did:\n{counters}".format(
                query=stump, counters=str(summary.counters),
                time=summary.result_available_after,
            ))

        return records, summary, keys
    
    def getWords(self, verbose = False):
        return self.query("MATCH (n:Word) RETURN n", verbose)
    
    def getLinks(self, verbose = False):
        return self.query("MATCH (:Word)-[l:WITH]-(:Word) return l", verbose)
    
    def seenWords(self, words: list[str], verbose = False) -> None:

        for chunk in divide_chunks(words, 5):

            # Format
            q = "".join([
                f'MERGE (n{i}:Word {{name:"{chunk[i]}"}}) ON MATCH SET n{i}.count = n{i}.count + 1, n{i}.decayed = n{i}.decayed + 1.0, n{i}.{self.datestr} = n{i}.{self.datestr} + 1 ON CREATE SET n{i}.count = 1, n{i}.decayed = 1.0, n{i}.{self.datestr} = 1 '
                for i in range(0, len(chunk))
            ])

            try:
                self.query(q, verbose)
            except:
                pass

        return None
    
    def seenLinks(self, pairs: list[tuple[str, str]], verbose = False) -> None:

        for chunk in divide_chunks(pairs, 5):
        
            # Format
            q = "".join([
                f'MATCH (n{i}:Word {{name:"{chunk[i][0]}"}}) MATCH (m{i}:Word {{name:"{chunk[i][1]}"}}) '
                for i in range(0, len(chunk))
            ])
            q += "".join([
                f'MERGE (n{i}) -[l{i}:WITH]- (m{i}) ON MATCH SET l{i}.count = l{i}.count + 1, l{i}.decayed = l{i}.decayed + 1.0, l{i}.{self.datestr} = l{i}.{self.datestr} + 1 ON CREATE SET l{i}.count = 1, l{i}.decayed = 1.0, l{i}.{self.datestr} = 1 '
                for i in range(0, len(chunk))
            ])

            try:
                self.query(q, verbose)
            except:
                pass

        return None
    
    def decay(self, c: int, verbose = False) -> None:
        self.query(f"MATCH (n:Word) SET n.decayed = n.decayed * {c}", verbose)
        self.query(f"MATCH (:Word)-[l:WITH]-() SET l.decayed = l.decayed * {c}", verbose)
        return None

    def close(self) -> None:
        self.driver.close()
        return None

