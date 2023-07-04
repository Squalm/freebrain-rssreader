# Runs on python 3.8 (not 3.9 at time of writing), because of Feedparser's requirements
import json, requests

import logging
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

from requests.api import request

def getSecret():
    print('Read secrets.txt...')
    with open('secrets.txt', mode='r') as file:
        return ("neo4j", [line for line in file][0])

URI = "neo4j+s://11a08269.databases.neo4j.io"
secret = getSecret()

with GraphDatabase.driver(URI, auth=("neo4j", secret)) as driver:
    driver.verify_connectivity()

# Grab links from db
def getLinks(SECRET:str, url = 'http://localhost:1337/v1/graphql'):
    print('Getting links from the db...')
    request_url = url
    request_headers = {
        'content-type': 'application/json',
        'X-HASURA-ADMIN-SECRET': SECRET
    }
    request_query = """query get_links {
        links {
            id
            link
            published
        }
    }
    """
    response = requests.post(request_url, json={'query': request_query}, headers=request_headers)
    print('Server says:', response.status_code)
    return json.loads(response.text)

# Grab words from db
def getWords(SECRET:str, url = 'http://localhost:1337/v1/graphql'):
    print('Getting keywords from the db...')
    request_url = url
    request_headers = {
        'content-type': 'application/json',
        'X-HASURA-ADMIN-SECRET': SECRET
    }
    request_query = """query get_keywords {
        keywords {
            id
            name
        }
    }
    """
    response = requests.post(request_url, json={'query': request_query}, headers=request_headers)
    print('Server says:', response.status_code)
    return json.loads(response.text)

# Grab joins from the db
def getJoins(SECRET:str, url = 'http://localhost:1337/v1/graphql'):
    print('Getting joins from the db...')
    request_url = url
    request_headers = {
        'content-type': 'application/json',
        'X-HASURA-ADMIN-SECRET': SECRET
    }
    request_query = """query get_joins {
        links_join_keywords {
            id
            link_id
            keyword_id
        }
    }
    """
    response = requests.post(request_url, json={'query': request_query}, headers=request_headers)
    print('Server says:', response.status_code)
    return json.loads(response.text)
