![server01 grabpy rssreading](https://cronitor.io/badges/1WB0lq/production/rKiHq3sbHR25W7MhOWOEZyWud0Y.svg)

# Freebrain - RSS Feed reader
For [Interbrain](https://interbrain.xhirp.com/). Reads a list of RSS feeds from [`feeds.csv`](https://github.com/Squalm/freebrain-rssreader/blob/main/feeds.csv) and pulls out the title and link. It breaks down the titles into words and aims to only get the most today's articles and merges them into the freebrain db using Cypher (locally hosted [neo4j](https://neo4j.com/) db). It also adds the connections between words (if they occured in the same article title) and adds those relationships to the db too.  

I run this in python `3.9.5`.

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
