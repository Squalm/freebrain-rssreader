import HTTP

# Get secret from db

# Get link from db
function getLinks(SECRET)

    query = "query { links { id link published } }"
    r = HTTP.request("GET", "https://free-brain.hasura.app/v1/graphql", [X_HASURA_ADMIN_SECRET = SECRET], query)
    println(r.status)
    println(r.body)
    return r

end # function

links = getLink()
println(links)