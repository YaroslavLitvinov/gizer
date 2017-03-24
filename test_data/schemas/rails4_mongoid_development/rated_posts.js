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
			"user_info":{
			    "name":"STRING",
			    "last_name":"STRING"
			},
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
 	"item_rates":[{
		"_id": {
			"oid": "STRING",
		    "bsontype": "INT"
		 },
		"name" : "STRING",
		"description" : "STRING",
		"created_at": "TIMESTAMP",
		"updated_at": "TIMESTAMP"
	}],
   "title": "STRING",
    "body": "STRING",
    "user_id": "STRING",
    "updated_at": "TIMESTAMP",
    "created_at": "TIMESTAMP",
    "tests" : [ "INT" ],
    "number":"INT",
    "enclosed":{
		"_id": {
			"oid": "STRING",
		    "bsontype": "INT"
		},
    	"field1":"STRING",
    	"field2":"INT",
    	"field3":"TIMESTAMP",
    	"field_array": [ "STRING" ]
    },
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
		"updated_at": "TIMESTAMP",
		"a_filed_with_id":{
			"oid": "STRING",
		    "bsontype": "INT"
		},
		"another_filed_with_id2":{
			"onemore_enclosed_level":{
				"oid": "STRING",
		    	"bsontype": "INT"
			},
			"some_strange_field":"INT"
		}
	}]
}
