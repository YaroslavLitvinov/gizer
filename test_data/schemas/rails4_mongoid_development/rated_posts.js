{
    "comments": [{
        "_id": {
            "oid": "STRING",
            "bsontype": "INT"
        },
        "body": "STRING",
        "updated_at": "TIMESTAMP",
        "created_at": "TIMESTAMP",
        "tests" : [ "INT" ],
		"rates" : [{
	        "_id": {
	            "oid": "STRING",
	            "bsontype": "INT"
	        },
			"user_id" : "STRING",
			"rate" : "STRING",
		    "created_at": "TIMESTAMP",
		    "updated_at": "TIMESTAMP",
			"item_rates":[{
		        "_id": {
		            "oid": "STRING",
		            "bsontype": "INT"
		        },
				"name" : "STRING",
				"description" : "STRING",
			    "created_at": "TIMESTAMP",
			    "updated_at": "TIMESTAMP"
			}]
		}]
    }],
    "title": "STRING",
    "body": "STRING",
    "user_id": "STRING",
    "updated_at": "TIMESTAMP",
    "created_at": "TIMESTAMP",
    "_id": {
        "oid": "STRING",
        "bsontype": "INT"
    },
	"rates":[{
		"_id": {
		    "oid": "STRING",
		    "bsontype": "INT"
	    },
		"user_id" : "STRING",
		"rate" : "INT",
		"created_at": "TIMESTAMP",
		"updated_at": "TIMESTAMP"
	}]
}
