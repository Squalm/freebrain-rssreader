# Runs on python 3.8 (not 3.9 at time of writing), because of Feedparser's requirements

import feedparser, csv, json, requests, re, time, sched

from tqdm import tqdm

from requests.api import request

from main import * # import functions from main

GRAPHQL = 'https://vkfwvlxduoseejskgrxg.hasura.eu-central-1.nhost.run/v1/graphql'

# Grab admin secret
print('Read secrets.txt...')
with open('secrets.txt', mode='r') as file:
    X_HASURA_ADMIN_SECRET = [line for line in file][0]

# Get feeds
print("Read feeds.csv...")
with open('feeds.csv', mode='r') as file:

    csvfile = csv.DictReader(file)
    #print(csvfile)

    urls = [line['feedurl'] for line in csvfile]

# for chunking later
def divide_chunks(l, n):
     
    for i in range(0, len(l), n):
        yield l[i:i + n]

# Loop on this infinitely so if we finish get new RSS feeds
while True:
        
    # Get RSS
    print("Getting RSS feeds...")

    parsed_response_links = getLinks(X_HASURA_ADMIN_SECRET, GRAPHQL)

    parsed_response_words = getWords(X_HASURA_ADMIN_SECRET, GRAPHQL)

    parsed_response_joins = getJoins(X_HASURA_ADMIN_SECRET, GRAPHQL)

    feed_entries_by_join = []
    feed_entries_by_links = []
    feed_entries_by_words = []
    _l = [l['link'] for l in parsed_response_links['data']['links']]
    for url in tqdm(urls):
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                
                if entry.link not in _l: # early check to see if we already have the link.

                    feed_entries_by_links.append(entry.link)

                    for word in entry.title.split():
                        word_stripped = re.sub('[\W_]+', '', word.lower())
                        if word_stripped != "":
                            feed_entries_by_join.append([entry.link, word_stripped])
                            if word_stripped not in feed_entries_by_words:
                                feed_entries_by_words.append(word_stripped)
        
        except:
            print("Bad URL:", url)
    # Remove duplicates
    feed_entries_by_links = list( dict.fromkeys(feed_entries_by_links) )
    feed_entries_by_words = list( dict.fromkeys(feed_entries_by_words) )

    print('Got', len(feed_entries_by_links), 'links,', len(feed_entries_by_words), 'words and', len(feed_entries_by_join), 'joins')

    # Compare links against the db for each and if doesn't exist, add it
    print('Removing links that already exist...')

    _links_already_in_db = [link_pair['link'] for link_pair in parsed_response_links['data']['links']]

    _cleaned_feed_entries_by_links = []
    for i in range(len(feed_entries_by_links)):
        if feed_entries_by_links[i] not in _links_already_in_db:
            _cleaned_feed_entries_by_links.append(feed_entries_by_links[i])
    feed_entries_by_links = _cleaned_feed_entries_by_links
    print('Found', len(feed_entries_by_links), 'new links')

    print('Add links to the db...')
    links_formatted_for_insert = ""
    for link in feed_entries_by_links:
        links_formatted_for_insert += '\n           {link: "' + link + '"},'

    links_formatted_for_insert = links_formatted_for_insert[0:len(links_formatted_for_insert)-1]
    #print(links_formatted_for_insert)

    request_url = GRAPHQL
    request_headers = {
        'content-type': 'application/json',
        'X-HASURA-ADMIN-SECRET': X_HASURA_ADMIN_SECRET
    }
    request_query = """mutation add_links {
        insert_links(
            objects: [
    """ + links_formatted_for_insert + """
            ]
        ) {
            affected_rows
        }
    }
    """

    response = requests.post(request_url, json={'query': request_query}, headers=request_headers)
    print('Server says:', response.status_code)
    print(response.text)

    # Compare words to db, if doesn't exist, add it
    _words_already_in_db = [word_pair['name'] for word_pair in parsed_response_words['data']['keywords']]
    _cleaned_feed_entries_by_words = []

    print("Removing words that already exist...")
    for i in range(len(feed_entries_by_words)):
        if feed_entries_by_words[i] not in _words_already_in_db:
            _cleaned_feed_entries_by_words.append(feed_entries_by_words[i])
    feed_entries_by_words = _cleaned_feed_entries_by_words
    print('Found', len(feed_entries_by_words), 'new words')

    print('Add words to the db...')
    words_formatted_for_insert = ""
    for word in feed_entries_by_words:
        words_formatted_for_insert += '\n           {name: "' + word + '"},'

    words_formatted_for_insert = words_formatted_for_insert[0:len(words_formatted_for_insert)-1]

    request_query = """mutation add_keywords {
        insert_keywords(
            objects: [
    """ + words_formatted_for_insert + """
            ]
        ) {
            affected_rows
        }
    }
    """

    response = requests.post(request_url, json={'query': request_query}, headers=request_headers)
    print('Server says:', response.status_code)
    print(response.text)

    # Add joins to db
    print("Clean joins...")
    _cleaned_feed_entries_by_join = []
    for join in feed_entries_by_join:
        if join not in _cleaned_feed_entries_by_join:
            _cleaned_feed_entries_by_join.append(join)
    feed_entries_by_join = _cleaned_feed_entries_by_join
    print('Found', len(feed_entries_by_join), 'unique joins')

    # Substitute ids
    # We must get keywords and links again so that we have the correct ids
    print('Substituting IDs... (first we must get all words and links again)')
    all_words = getWords(X_HASURA_ADMIN_SECRET, GRAPHQL)
    words_in_db = {}
    for word_pair in all_words['data']['keywords']:
        words_in_db[word_pair['name']] = word_pair['id']

    all_links = getLinks(X_HASURA_ADMIN_SECRET, GRAPHQL)
    links_in_db = {}
    for link_pair in all_links['data']['links']:
        links_in_db[link_pair['link']] = link_pair['id']

    print('Substituting...')
    for i in range(len(feed_entries_by_join)):
        feed_entries_by_join[i][0] = links_in_db[feed_entries_by_join[i][0]]
        feed_entries_by_join[i][1] = words_in_db[feed_entries_by_join[i][1]]

    joins_already_in_db = [[join['link_id'], join['keyword_id']] for join in parsed_response_joins['data']['links_join_keywords']]
    #print(joins_already_in_db)

    _cleaned_feed_entries_by_join = []
    for join in tqdm( feed_entries_by_join ):
        if join not in joins_already_in_db:
            _cleaned_feed_entries_by_join.append(join)
    feed_entries_by_join = _cleaned_feed_entries_by_join
    print('Found', len(feed_entries_by_join), 'new joins')

    print('Add joins to the db...')
    joins_formatted_for_insert = []
    for j in feed_entries_by_join:
        joins_formatted_for_insert.append('{link_id: "' + str(j[0]) + '", keyword_id: "' + str(j[1]) + '"}, ')

    joins_formatted_for_insert = joins_formatted_for_insert[0:len(joins_formatted_for_insert)-1]

    # Split into long lumps when addings these!
    chunks = list(divide_chunks(joins_formatted_for_insert, 5000))

    for chunk in chunks:

        request_query = """mutation add_joins {
            insert_links_join_keywords(
                objects: [
        """ + "".join(chunk) + """
                ]
            ) {
                affected_rows
            }
        }
        """
        response = requests.post(request_url, json={'query': request_query}, headers=request_headers)
        print('Server says:', response.status_code)
        print(response.text)

        time.sleep(10)

    # Do stats!
    print("Doing Stats")

    keyword_count = len(all_words["data"]["keywords"])
    link_count = len(all_links["data"]["links"])
    join_count = len(parsed_response_joins["data"]["links_join_keywords"])

    request_query = """mutation add_stats {
    insert_stats_one(object: {joins: """+str(join_count)+""", keywords: """+str(keyword_count)+""", links: """+str(link_count)+"""}) {
        joins
        keywords
        links
        time
    }
    }"""
    
    response = requests.post(request_url, json={'query': request_query}, headers=request_headers)
    print('Server says:', response.status_code)
    print(response.text)

    print('Sleeping for 12 hours.\n')
    time.sleep(3600 * 12)
