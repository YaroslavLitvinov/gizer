[
{
  "ts": {"$timestamp": {"t": 1000000012, "i": 1}},
  "t": 2,
  "h": "1507724081854138926",
  "v": 2,
  "op": "u",
  "ns": "rails4_mongoid_development.posts",
  "o2": {
    "_id": { "$oid": "56b8da59f9fcee1b00000007" }
  },
  "o": {
    "$set": {
      "comments": [
        {
          "_id": { "$oid": "56b8f344f9fcee1b00000018" },
          "updated_at": { "$date" : "2016-02-08T19:57:56.678Z"},
          "created_at": { "$date" : "2016-02-08T19:57:56.678Z"}
        },
        {
          "_id": { "$oid": "56b8f35ef9fcee1b0000001a" },
          "updated_at": { "$date" : "2016-02-08T19:58:22.847Z"},
          "created_at": { "$date" : "2016-02-08T19:58:22.847Z"}
        }
      ]
    }
  }
},
{
  "ts": {"$timestamp": {"t": 1000000015, "i": 1}},
  "t": 2,
  "h": "1507724081854138926",
  "v": 2,
  "op": "u",
  "ns": "rails4_mongoid_development.posts",
  "o2": {
    "_id": { "$oid": "56b8da59f9fcee1b00000007" }
  },
  "o": {
    "$set": {
      "comments": [
        {
          "_id": { "$oid": "000000000000000000000018" },
          "updated_at": { "$date" : "2016-02-08T19:57:56.678Z"},
          "created_at": { "$date" : "2016-02-08T19:57:56.678Z"}
        },
        {
          "_id": { "$oid": "000000000000000000000019" },
          "updated_at": { "$date" : "2016-02-08T19:58:22.847Z"},
          "created_at": { "$date" : "2016-02-08T19:58:22.847Z"}
        }
      ]
    }
  }
}
]