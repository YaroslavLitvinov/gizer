[
{
    "_id": { "$oid": "56b8f05cf9fcee1b00001000" },
    "title": "The Gaslo!",
    "body": "Glory for Ukraine!",
    "user_id": "56b8d7caf9fcee1b00000001",
    "updated_at": { "$date" : "2016-02-08T21:01:00.985Z"},
    "created_at": { "$date" : "2016-02-08T21:00:00.985Z"},
	"tests": [10,20,30],
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
	],
	"rates": [
		{
			"_id": { "$oid": "56b8f05cf9fcee1b00002000" },
			"user_id" : "56b8d7caf9fcee1b00000002",
			"rate" : 10,
		    "created_at": { "$date" : "2016-02-08T21:00:00.985Z"},
		    "updated_at": { "$date" : "2016-02-08T21:00:00.985Z"}
		}
	]
}
]
