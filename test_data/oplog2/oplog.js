[
{

  "ts": {"$timestamp": {"t": 1164278288, "i": 2}},
  "op": "i",
  "ns": "rails4_mongoid_development.guests",
  "o": { 
      "id": 13, 
      "comment": "foo"
  }
},
{
  "ts": {"$timestamp": {"t": 1164278289, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1264278289, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1364278289, "i": 1}},
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
        }
      ]
    }
  }
},
{
  "ts": {"$timestamp": {"t": 1464278289, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1564278289, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1664278289, "i": 1}},
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
        "updated_at": { "$date" : "2016-02-08T19:42:33.589Z"},
        "created_at": { "$date" : "2016-02-08T19:42:33.589Z"}
      }
    }
  }
},
{
  "ts": {"$timestamp": {"t": 1764278289, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1864278289, "i": 1}},
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
      "body":"commments2222_2",
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
  "ts": {"$timestamp": {"t": 1964278289, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1974278289, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1984278289, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1994278289, "i": 1}},
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
      "updated_at": { "$date" : "2016-02-08T20:02:12.985Z"}
    }
  }
}
,
{

  "ts": {"$timestamp": {"t": 1994278289, "i": 3}},
  "op": "i",
  "ns": "rails4_mongoid_development.guests",
  "o": { 
      "id": 777, 
      "comment": "test"
  }
}

]
