[
{
  "ts": {"$timestamp": {"t": 1000000001, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1000000002, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1000000003, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1000000004, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1000000005, "i": 1}},
  "t": 2,
  "h": "8307152860560417908",
  "v": 2,
  "op": "u",
  "ns": "rails4_mongoid_development.posts2",
  "o2": {
    "_id": 33
  },
  "o": {
    "$set": {
	"body": "body4"
    }
  }
},
{
  "ts": {"$timestamp": {"t": 1000000006, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1000000007, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1000000008, "i": 1}},
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
  "ts": {"$timestamp": {"t": 1000000009, "i": 1}},
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
},
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
},
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
  "ts": {"$timestamp": {"t": 1000000013, "i": 1}},
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
},
{
  "ts": {"$timestamp": {"t": 1000000014, "i": 1}},
  "t": 2,
  "h": "-3907160829967975359",
  "v": 2,
  "op": "u",
  "ns": "rails4_mongoid_development.rated_posts",
  "o2": {
    "_id": { "$oid": "56b8f05cf9fcee1b00001000" }
  },
  "o": {
    "$set": {
	    "title": "The Gaslo!",
	    "updated_at": { "$date" : "2016-02-08T21:01:00.985Z"}
    }
  }
},
{
  "ts": {"$timestamp": {"t": 1000000015, "i": 1}},
  "t": 2,
  "h": "-3907160829967975359",
  "v": 2,
  "op": "u",
  "ns": "rails4_mongoid_development.rated_posts",
  "o2": {"_id": { "$oid": "56b8f05cf9fcee1b00001000" } },
  "o": {
    "$set": {
	    "comments": [
			{
				"_id": { "$oid": "56b8f05cf9fcee1b00002000" },
				"user_id" : "56b8d7caf9fcee1b00000002",
		        "body": "Good post!",
			    "updated_at": { "$date" : "2016-02-08T21:00:00.985Z"},
			    "created_at": { "$date" : "2016-02-08T21:00:00.985Z"},
		        "tests" : [ 1,2,3 ],
				"rates" : [
					{
						"_id": { "$oid": "56b8f05cf9fcee1b00003000" ,"bsontype": 3},
						"user_id" : "56b8d7caf9fcee1b00000003",
						"rate" : 10,
					    "updated_at": { "$date" : "2016-02-08T21:00:00.985Z"},
					    "created_at": { "$date" : "2016-02-08T21:00:00.985Z"}
					}
				]
    		},
			{
				"_id": { "$oid": "56b8f05cf9fcee1b00002001" },
				"user_id" : "56b8d7caf9fcee1b00000004",
		        "body": "Glory for Heroes!",
			    "updated_at": { "$date" : "2016-02-08T21:02:00.985Z"},
			    "created_at": { "$date" : "2016-02-08T21:02:00.985Z"},
		        "tests" : [ 1,2,3 ],
				"additional_filed":"OTAKE!",
				"rates" : [
					{
						"_id": { "$oid": "56b8f05cf9fcee1b00003000" ,"bsontype": 3},
						"user_id" : "56b8d7caf9fcee1b00000004",
						"rate" : 100,
					    "updated_at": { "$date" : "2016-02-08T21:00:00.985Z"},
					    "created_at": { "$date" : "2016-02-08T21:00:00.985Z"}
					}
				]
	    	}
		]
    }
  }
},
{
  "ts": {"$timestamp": {"t": 1000000016, "i": 1}},
  "t": 2,
  "h": "-3907160829967975359",
  "v": 2,
  "op": "u",
  "ns": "rails4_mongoid_development.rated_posts",
  "o2": {
    "_id": { "$oid": "56b8f05cf9fcee1b00001000" }
  },
  "o": {
    "$set": {
		"additional_field":"OTAKE!",
	    "updated_at": { "$date" : "2016-02-08T21:01:00.985Z"}
    }
  }
},
{
  "ts": {"$timestamp": {"t": 1000000017, "i": 1}},
  "t": 2,
  "h": "-3907160829967975359",
  "v": 2,
  "op": "d",
  "ns": "rails4_mongoid_development.rated_posts",
  "b" : true,
  "o" : {
    "_id" : {"$oid":"56b8f05cf9fcee1b00011000"}
  }
}
]
