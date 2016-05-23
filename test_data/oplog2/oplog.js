[
{
  "ts": "6249008760904220673",
  "t": 2,
  "h": "6498594270312035435",
  "v": 2,
  "op": "i",
  "ns": "rails4_mongoid_development.posts",
  "o": {
    "_id": { "$oid": "56b8f05cf9fcee1b00000010" },
    "title": "title3",
    "body": "body3",
    "user_id": "56b8d7caf9fcee1b00000001",
    "updated_at": { "$date" : "2016-02-08T19:45:32.501Z"},
    "created_at": { "$date" : "2016-02-08T19:45:32.501Z"}
  }
},
{
  "ts": "6249012068029138000",
  "t": 2,
  "h": "6500669658301126550",
  "v": 2,
  "op": "i",
  "ns": "rails4_mongoid_development.posts",
  "o": {
      "_id": { "$oid": "56b8da59f9fcee1b00000007" }
  }
},
{
  "ts": "6249012068029138593",
  "t": 2,
  "h": "6500669658302126550",
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
          "updated_at": {"$date": "2016-02-08T19:57:56.678Z"},
	  "created_at": {"$date": "2016-02-08T19:57:56.678Z"}
        },
        {
        }
      ]
    }
  }
},
{
  "ts": "6249012828238249985",
  "t": 2,
  "h": "8307152860560417908",
  "v": 2,
  "op": "u",
  "ns": "rails4_mongoid_development.posts",
  "o2": {
    "_id": { "$oid": "56b8da59f9fcee1b00000007" }
  },
  "o": {
    "$set": {
	"comments.2.tests.1": 101
    }
  }
},
{
  "ts": "6249010526135779329",
  "t": 2,
  "h": "7673062055086646593",
  "v": 2,
  "op": "u",
  "ns": "rails4_mongoid_development.posts",
  "o2": {
    "_id": { "$oid": "56b8da59f9fcee1b00000007" }
  },
  "o": {
    "$set": {
      "updated_at": { "$date" :"2016-02-08T19:52:23.883Z"}
    }
  }
},
{
  "ts": "6249007992105074689",
  "t": 2,
  "h": "-4894171422577715544",
  "v": 2,
  "op": "u",
  "ns": "rails4_mongoid_development.posts",
  "o2": {
    "_id": { "$oid": "56b8da51f9fcee1b00000006" }
  },
  "o": {
    "$set": {
      "comments.6": {
        "_id": { "$oid": "56b8efa9f9fcee1b0000000f" },
        "body": "comment6",
        "updated_at": "2016-02-08T19:42:33.589Z",
        "created_at": "2016-02-08T19:42:33.589Z"
      }
    }
  }
},
{
  "ts": "6249009615602712577",
  "t": 2,
  "h": "-3308614633402865095",
  "v": 2,
  "op": "d",
  "ns": "rails4_mongoid_development.posts",
  "o": {
    "_id": { "$oid": "56b8da51f9fcee1b00000006" }
  }
},
{
  "ts": "6249011956359888897",
  "t": 2,
  "h": "7863054063887715514",
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
          "updated_at": { "$date" :"2016-02-08T19:57:56.678Z"},
          "created_at": { "$date" :"2016-02-08T19:57:56.678Z"}
        }
      ]
    }
  }
},
{
  "ts": "6249011999309561857",
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
},
{
  "ts": "6249012068029038593",
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
},
{
  "ts": "6249012252712632321",
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
          "updated_at": "2016-02-08T19:57:56.678Z",
          "created_at": "2016-02-08T19:57:56.678Z"
        },
        {
          "_id": { "$oid": "56b8f35ef9fcee1b0000001a" },
          "updated_at": "2016-02-08T19:58:22.847Z",
          "created_at": "2016-02-08T19:58:22.847Z"
        }
      ]
    }
  }
},
{
  "ts": "6249013055871516673",
  "t": 2,
  "h": "-3907160829967975359",
  "v": 2,
  "op": "u",
  "ns": "rails4_mongoid_development.posts",
  "o2": {
    "_id": { "$oid": "56b8f05cf9fcee1b00000010" }
  },
  "o": {
    "$set": {
      "title": "sada",
      "updated_at": "2016-02-08T20:02:12.985Z"
    }
  }
}
]
