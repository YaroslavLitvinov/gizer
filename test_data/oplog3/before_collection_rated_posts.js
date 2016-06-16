[
{
    "_id": { "$oid": "56b8f05cf9fcee1b00001000" },
    "title": "This is the title for the post",
    "body": "Glory for Ukraine!",
    "user_id": "56b8d7caf9fcee1b00000001",
    "updated_at": { "$date" : "2016-02-08T21:00:00.985Z"},
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
},
{
    "_id": { "$oid": "56b8f05cf9fcee1b00011000" },
    "title": "text to be removed",
    "body": "text to be removed",
    "user_id": "56b8d7caf9fcee1b00000001",
    "updated_at": { "$date" : "2016-03-08T21:00:00.985Z"},
    "created_at": { "$date" : "2016-03-08T21:00:00.985Z"},
	"tests": [100,200,300],
    "comments": [
		{
			"_id": { "$oid": "56b8f05cf9fcee1b00012000" },
			"user_id" : "56b8d7caf9fcee1b00000002",
	        "body": "Excellent!",
		    "updated_at": { "$date" : "2016-03-08T21:00:00.985Z"},
		    "created_at": { "$date" : "2016-03-08T21:00:00.985Z"},
	        "tests" : [ 101,202,303 ],
			"rates" : [
				{
					"_id": { "$oid": "56b8f05cf9fcee1b00013000" ,"bsontype": 3},
					"user_id" : "56b8d7caf9fcee1b00000003",
					"rate" : 2222,
				    "updated_at": { "$date" : "2016-03-08T21:00:00.985Z"},
				    "created_at": { "$date" : "2016-03-08T21:00:00.985Z"}
				}
			]
    	}
	],
	"rates": [
		{
			"_id": { "$oid": "56b8f05cf9fcee1b00012000" },
			"user_id" : "56b8d7caf9fcee1b00000002",
			"rate" : 101,
		    "created_at": { "$date" : "2016-03-08T21:00:00.985Z"},
		    "updated_at": { "$date" : "2016-03-08T21:00:00.985Z"}
		}
	]
}
]
