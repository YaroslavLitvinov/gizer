[
{
  "ts": {"$timestamp": {"t": 1000000011, "i": 1}},
  "t": 2,
  "h": "8307152860560416908",
  "v": 2,
  "op": "u",
  "ns": "rails4_mongoid_development.posts",
  "o2": {
    "_id": { "$oid": "56b8da59f9fcee1b00000007" }
  },
  "o": {
    "$set": {
      "comments.3": {
        "_id": { "$oid": "56b8f35ef9fcee1b0000001a" },
        "updated_at": {"$date": "2016-02-08T19:58:22.847Z"},
	    "created_at": {"$date": "2016-02-08T19:58:22.847Z"}
      }
    }
  }
}
]