[
{
    "_id": { "$oid": "56b8f05cf9fcee1b00001000" },
    "title": "The title",
    "body": "Glory for Ukraine!",
    "user_id": "56b8d7caf9fcee1b00000001",
    "updated_at": { "$date" : "2016-02-08T19:45:32.501Z"},
    "created_at": { "$date" : "2016-02-08T19:45:32.501Z"},
	"tests": [10,20,30],
    "comments": [
		{
			"_id": { "$oid": "56b8f05cf9fcee1b00002000" },
			"user_id" : "56b8d7caf9fcee1b00000002",
	        "body": "Good post!",
		    "updated_at": { "$date" : "2016-02-08T19:45:32.501Z"},
		    "created_at": { "$date" : "2016-02-08T19:45:32.501Z"},
	        "tests" : [ 1,2,3 ],
			"rates" : [
				{
					"_id": { "$oid": "56b8f05cf9fcee1b00003000" ,"bsontype": 3},
					"user_id" : "56b8d7caf9fcee1b00000003",
					"rate" : 10,
				    "updated_at": { "$date" : "2016-02-08T19:45:32.501Z"},
				    "created_at": { "$date" : "2016-02-08T19:45:32.501Z"}
				}
			]
    	}
	],
	"rates": [
		{
			"_id": { "$oid": "56b8f05cf9fcee1b00002000" },
			"user_id" : "56b8d7caf9fcee1b00000002",
			"rate" : 10,
		    "created_at": { "$date" : "2016-02-08T19:45:32.501Z"},
		    "updated_at": { "$date" : "2016-02-08T19:45:32.501Z"}	
		}
	]
}
]
