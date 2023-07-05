# Freebrain - RSS Feed reader
For the freebrain/interbrain (name is WIP) project. Reads a list of RSS feeds from [`feeds.csv`](https://github.com/Squalm/freebrain-rssreader/blob/main/feeds.csv) and pulls out the title and link. It checks the links for duplicates (against itself, and the db) and pushes them to the freebrain db using GraphQL (hosted by [nhost](https://github.com/nhost/nhost)). It then breaks down the titles into words (makes them lowercase, removes special characters, checks for duplicates etc.) and pushes them to the feedbrain db. It then takes the joins between the words and links (i.e. this word was in the title of this link) and substitutes the db ids (after pulling all the words and links again, so that the ids are correct) then pushes that up to the freebrain db.  

N.B. `feedparser` (the python module) is a dependency for this project and does not (as of 26/08/21) support `python 3.9`; I wrote it in `3.7.7`.

## RSS Feeds
We can't guarantee we'll catch everything from these sites, but we get as much as we can. If you wanna see the full list of RSS feeds we check, you can look at [this file](https://github.com/Squalm/freebrain-rssreader/blob/main/feeds.csv).

- BBC
- The Guardian
- New York Times
- CNN
- Fox
- Washington Post
- Daily Mail
- The Independent
- Huffington Post
- PCGamer
- Polygon
- The Sun
- Mirror
- NPR
- USA Today
- Politico
- Metro
- MyLondon
- Birmingham Mail
- Glasgow Times
- Liverpool Echo
- Bristol Post
- The Manchester Evening News
- Sheffield Telegraph
- Leeds Live
- Edinburgh News (scotsman)
- Edinburgh Live
- Leicester Mercury
- New York Post
- Los Angeles Times
- Click2Houston
- AZ News Now
- Philly Voice
- San Antonia Current
- San Diego Union Tribune
- Dallas News
- Mercury News
