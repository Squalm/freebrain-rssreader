# Runs on python 3.8 (not 3.9 at time of writing), because of Feedparser's requirements

from telnetlib import XASCII
import csv, requests, time, sched

from requests.api import request

from main import * # import functions from main

GRAPHQL = 'https://vkfwvlxduoseejskgrxg.hasura.eu-central-1.nhost.run/v1/graphql'

# Grab admin secret
print('Read secrets.txt...')
with open('secrets.txt', mode='r') as file:
    X_HASURA_ADMIN_SECRET = [line for line in file][0]

# Calculate the counts for a word id
def calc_counts(word_id):

    relevant_links = [join['link_id'] for join in filtered_joins if join['keyword_id'] == word_id]
    
    flat_joins = sum([[join for join in filtered_joins if join['link_id'] == link] for link in relevant_links], [])
    
    counts = [(w["id"], [join['keyword_id'] for join in flat_joins].count(w["id"])) for w in words_in_db]

    cs = sorted(counts, key = lambda x: x[1], reverse=True) # short for counts sorted
    
    # Perpare for mutation
    request_query = 'mutation update_count {update_keywords(where: {id: {_eq: "' + str(word_id) + '"} }, _set: {'

    request_query += 'associated_1: "' + str(cs[1][0]) + '", associated_1_count: ' + str(cs[1][1])
    for x in range(2, 11):
        if cs[x][1] > 0:
            request_query += ', associated_' + str(x) + ': "' + str(cs[x][0]) + '", associated_' + str(x) + "_count: " + str(cs[x][1])

    request_query += "}) { affected_rows } }"

    return request_query

# Submit the count to the url
def submit_count(url, headers, query):
    response = requests.post(url, json={'query': query}, headers=headers)
    #print('Server says:', response.status_code)
    return response.text

# combines calc_counts and submit_count
def count_word_full(sc, word_list, id, url, headers, prevtime):

    print(id, "of", len(word_list), ":", end=" ")

    if id < len(word_list) -1:
        s.enter(1, 1, count_word_full, (sc, word_list, id+1, url, headers, time.time(),))

    response = submit_count(url, headers, calc_counts(word_list[id]['id']))

    print("took", str(time.time() - prevtime)[0:4], ":", response, " #" + str(word_list[id]['name']))

while True:

    # Get all the stuff
    words_in_db = getWords(X_HASURA_ADMIN_SECRET, GRAPHQL)["data"]["keywords"]
    feed_entries_by_join = getJoins(X_HASURA_ADMIN_SECRET, GRAPHQL)['data']['links_join_keywords']

    # Filter words to exclude words we don't want
    print("Read excludedwords.csv...")
    with open('excludedwords.csv', mode='r') as file:

        csvfile = csv.DictReader(file)
        #print(csvfile)

        ex_words = [line['word'] for line in csvfile]

    print("Filter joins to exclude words we don't want...")
    ex_ids = [word['id'] for word in words_in_db if word['name'] in ex_words]
    filtered_joins = [join for join in feed_entries_by_join if join['keyword_id'] not in ex_ids]

    # Get links in db (for published dates)
    # links = getLinks(X_HASURA_ADMIN_SECRET)

    print('Counting', str(len(words_in_db)), 'words')

    print('Calculating counts... (and submit to db)')

    filter_counts_by_every_word = []
    request_url = GRAPHQL
    request_headers = {
        'content-type': 'application/json',
        'X-HASURA-ADMIN-SECRET': X_HASURA_ADMIN_SECRET
    }



    # sched setup
    s = sched.scheduler(time.time, time.sleep)

    length = len(words_in_db)

    # enter the first even to sched
    s.enter(1, 1, count_word_full, (s, words_in_db, 0, request_url, request_headers, time.time(),))
    s.run()