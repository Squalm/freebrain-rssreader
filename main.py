# Runs on python 3.7 (not 3.9 at time of writing), because of Feedparser's requirements

import feedparser, csv, json, requests, re, time, sched

from requests.api import request

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

# Grab links from db
def getLinks(SECRET:str):
    print('Getting links from the db...')
    request_url = 'https://free-brain.hasura.app/v1/graphql'
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
def getWords(SECRET:str):
    print('Getting keywords from the db...')
    request_url = 'https://free-brain.hasura.app/v1/graphql'
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
def getJoins(SECRET:str):
    print('Getting joins from the db...')
    request_url = 'https://free-brain.hasura.app/v1/graphql'
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

# Loop on this infinitely so if we finish get new RSS feeds
while True:
        
    # Get RSS
    print("Getting RSS feeds...")
    feed_entries_by_join = []
    feed_entries_by_links = []
    feed_entries_by_words = []
    for url in urls:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                
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

    # Keep this list for later
    words_affected = feed_entries_by_words

    print('Got', len(feed_entries_by_links), 'links,', len(feed_entries_by_words), 'words and', len(feed_entries_by_join), 'joins')

    parsed_response_links = getLinks(X_HASURA_ADMIN_SECRET)

    parsed_response_words = getWords(X_HASURA_ADMIN_SECRET)

    parsed_response_joins = getJoins(X_HASURA_ADMIN_SECRET)

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

    request_url = 'https://free-brain.hasura.app/v1/graphql'
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
    all_words = getWords(X_HASURA_ADMIN_SECRET)
    words_in_db = {}
    for word_pair in all_words['data']['keywords']:
        words_in_db[word_pair['name']] = word_pair['id']

    all_links = getLinks(X_HASURA_ADMIN_SECRET)
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
    for join in feed_entries_by_join:
        if join not in joins_already_in_db:
            _cleaned_feed_entries_by_join.append(join)
    feed_entries_by_join = _cleaned_feed_entries_by_join
    print('Found', len(feed_entries_by_join), 'new joins')

    print('Add joins to the db...')
    joins_formatted_for_insert = ""
    for join in feed_entries_by_join:
        joins_formatted_for_insert += '\n           {link_id: ' + str(join[0]) + ', keyword_id: ' + str(join[1]) + '},'

    joins_formatted_for_insert = joins_formatted_for_insert[0:len(joins_formatted_for_insert)-1]

    request_query = """mutation add_joins {
        insert_links_join_keywords(
            objects: [
    """ + joins_formatted_for_insert + """
            ]
        ) {
            affected_rows
        }
    }
    """
    response = requests.post(request_url, json={'query': request_query}, headers=request_headers)
    print('Server says:', response.status_code)
    print(response.text)

    print("All done.")

    # Gotta take joins and count the frequencies of each word
    feed_entries_by_join = getJoins(X_HASURA_ADMIN_SECRET)

    # Filter words to exclude words we don't want
    print("Read excludedwords.csv...")
    with open('excludedwords.csv', mode='r') as file:

        csvfile = csv.DictReader(file)
        #print(csvfile)

        ex_words = [line['word'] for line in csvfile]

    # Substitute ex_words for ids of those words
    print('Substituting...')
    ex_words = [words_in_db[i] for i in ex_words]

    print("Filter joins to exclude words we don't want...")
    feed_entries_by_join['data']['links_join_keywords'] = [join for join in feed_entries_by_join['data']['links_join_keywords'] if join['keyword_id'] not in ex_words]

    # Get links in db (for published dates)
    # links = getLinks(X_HASURA_ADMIN_SECRET)

    print('Rechecking', str(len(words_affected)), 'words')

    print("Substituting...")
    for i in range(len(words_affected)):
        words_affected[i] = words_in_db[words_affected[i]]

    print('Calculating counts... (and submit to db)')

    filter_counts_by_every_word = []
    request_url = 'https://free-brain.hasura.app/v1/graphql'
    request_headers = {
        'content-type': 'application/json',
        'X-HASURA-ADMIN-SECRET': X_HASURA_ADMIN_SECRET
    }

    # Calculate the counts for a word id
    def calc_counts(word_id):

        relevant_links = [join['link_id'] for join in feed_entries_by_join['data']['links_join_keywords'] if join['keyword_id'] == word_id]
        
        flat_joins = sum([[join for join in feed_entries_by_join['data']['links_join_keywords'] if join['link_id'] == link] for link in relevant_links], [])
        
        counts = [(w+1, [join['keyword_id'] for join in flat_joins].count(w+1)) for w in range(len(words_in_db))]

        cs = sorted(counts, key = lambda x: x[1], reverse=True) # short for counts sorted
        
        # Perpare for mutation
        request_query = "mutation update_count {update_keywords(where: {id: {_eq: " + str(word_id) + "} }, _set: {"

        request_query += "associated_1: " + str(cs[1][0]) + ", associated_1_count: " + str(cs[1][1])
        for x in range(2, 11):
            if cs[x][1] > 0:
                request_query += ", associated_" + str(x) + ": " + str(cs[x][0]) + ", associated_" + str(x) + "_count: " + str(cs[x][1])

        request_query += "}) { affected_rows } }"

        return request_query

    # Submit the count to the url
    def submit_count(url, headers, query):
        response = requests.post(url, json={'query': query}, headers=headers)
        #print('Server says:', response.status_code)
        return response.text

    # sched setup
    s = sched.scheduler(time.time, time.sleep)

    length = len(words_in_db)

    # combines calc_counts and submit_count
    def count_word_full(sc, word_list, id, url, headers, prevtime):

        print(id, "of", len(word_list), "#" + str(word_list[id]), ":", end=" ")

        if word_list[id] not in ex_words:

            if id < len(word_list) -1:
                s.enter(1, 1, count_word_full, (sc, word_list, id+1, url, headers, time.time(),))

            response = submit_count(url, headers, calc_counts(word_list[id]))

            print("took", str(time.time() - prevtime), ":", response)

        else:
            print("excluded")
            count_word_full(sc, word_list, id+1, url, headers, time.time())

    # enter the first even to sched
    s.enter(1, 1, count_word_full, (s, words_affected, 0, request_url, request_headers, time.time(),))
    s.run()