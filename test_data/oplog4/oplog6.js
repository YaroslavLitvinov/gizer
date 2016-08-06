[
{
  "ts": {"$timestamp": {"t": 1000000010, "i": 1}},
  "t": 2,
  "h": "5531998682355049033",
  "v": 2,
  "op": "u",
  "ns": "rails4_mongoid_development.posts",
  "o2": {
    "_id": { "$oid": "56b8da59f9fcee1b00000007" }
  },
  "o": {
    "$set": {
      "comments.2": {
        "_id": { "$oid": "56b8f34ef9fcee1b00000019" },
        "body": "commments2222",
        "updated_at": { "$date" :"2016-02-08T19:58:06.008Z"},
        "created_at": { "$date" :"2016-02-08T19:58:06.008Z"}
      }
    }
  }
}
]