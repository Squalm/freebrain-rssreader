# Runs on python 3.8 (not 3.9 at time of writing), because of Feedparser's requirements

import feedparser, csv, re, itertools

from tqdm import tqdm
from requests.api import request
from main import * # import functions from main

URI = "neo4j+s://11a08269.databases.neo4j.io"
# Grab admin secret
secret = getSecret()
# Connect to db
db = Graph(URI, ("neo4j", secret))

# Get feeds
print("Read feeds.csv...")
with open('feeds.csv', mode='r') as file:

    csvfile = csv.DictReader(file)
    urls = [line['feedurl'] for line in csvfile]

# Get excluded words
print("Read excludedwords.csv...")
with open('excludedwords.csv', mode='r') as file:

    csvfile = csv.DictReader(file)
    forbidden = [line['word'] for line in csvfile]

# Get RSS
print("Getting RSS feeds...")

joins = []
links = []
words = []
for url in tqdm(urls):
    try:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            
            published = getattr(entry, "published", getattr(entry, "pubDate", ""))

            # As an approximation of checking if the article was published today, 
            # check if it has the same day as the latest article published 
            # (since date format varies)
            if published == getattr(feed.entries[0], "published", getattr(feed.entries[0], "pubDate", "")):
                if entry.link not in links:

                    links.append(entry.link)

                    stripped = [
                        re.sub('[\W_]+', '', word.lower()) 
                        for word in entry.title.split() 
                        if re.sub('[\W_]+', '', word.lower()) != "" and
                        re.sub('[\W_]+', '', word.lower()) not in forbidden
                    ]

                    words += stripped

                    for pair in itertools.combinations(stripped, 2):
                        joins.append(pair)

    
    except:
        print("Bad URL:", url)

# Add words to db
print(f"Add {len(words)} words...")
db.seenWords(words, True)

# Add renlationships to db
print(f"Add {len(joins)} relationships...")
db.seenLinks(joins, True)
