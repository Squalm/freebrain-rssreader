# Runs on python 3.8 (not 3.9 at time of writing), because of Feedparser's requirements
import json, requests, os

import logging
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

from requests.api import request

# load .env
from dotenv import load_dotenv
load_dotenv()

def getSecret():
    return(os.environ["PASSWORD"])

# check connection
URI = "neo4j+s://11a08269.databases.neo4j.io"
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

    def query(self, query: str, verbose = False):
        records, summary, keys = self.driver.execute_query(query)
        if verbose:
            stump = summary.query if len(summary.query) <= 40 else summary.query[0:39] + "..."
            print("The query `{query}` returned {records_count} records in {time} ms.".format(
                query=stump, records_count=len(records),
                time=summary.result_available_after,
            ))

        return records, summary, keys
    
    def getWords(self, verbose = False):
        return self.query("MATCH (n:Word) RETURN n", verbose)
    
    def getLinks(self, verbose = False):
        return self.query("MATCH (:Word)-[l:WITH]-(:Word) return l", verbose)
    
    def seenWords(self, words: list[str], verbose = False) -> None:

        for chunk in divide_chunks(words, 500):

            # Format
            q = "".join([
                f'MERGE (n{i}:Word {{name:"{chunk[i]}"}}) ON MATCH SET n{i}.count = n{i}.count + 1 ON CREATE SET n{i}.count = 1 '
                for i in range(0, len(chunk))
            ])

            self.query(q, verbose)

        return None
    
    def seenLinks(self, pairs: list[tuple[str, str]], verbose = False) -> None:

        for chunk in divide_chunks(pairs, 100):
        
            # Format
            q = "".join([
                f'MATCH (n{i}:Word {{name:"{chunk[i][0]}"}}) MATCH (m{i}:Word {{name:"{chunk[i][1]}"}}) '
                for i in range(0, len(chunk))
            ])
            q += "".join([
                f'MERGE (n{i}) -[l{i}:WITH]- (m{i}) ON MATCH SET l{i}.count = l{i}.count + 1 ON CREATE SET l{i}.count = 1 '
                for i in range(0, len(chunk))
            ])

            self.query(q, verbose)

        return None

    def close(self):
        self.driver.close()

