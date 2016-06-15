__author__ = 'Volodymyr Varchuk'

schema = {
    "comments": [{
        "_id": {
            "oid": "STRING",
            "bsontype": "INT"
        },
        "body": "STRING",
        "updated_at": "TIMESTAMP",
        "created_at": "TIMESTAMP"
    }],
    "title": "STRING",
    "body": "STRING",
    "user_id": "STRING",
    "updated_at": "TIMESTAMP",
    "created_at": "TIMESTAMP",
    "_id": {
        "oid": "STRING",
        "bsontype": "INT"
    }
}

oplog_tz_info = '{"tzinfo_obj": { "$date" : "2016-02-08T19:42:33.589Z"}}'

oplog_u_01 = '''{
    "ts": "6249007992105074689",
    "t": 2,
    "h": "-4894171422577715544",
    "v": 2,
    "op": "u",
    "ns": "rails4_mongoid_development.posts",
    "o2": {
        "_id": {"$oid": "56b8da51f9fcee1b00000006"}
    },
    "o": {
        "$set": {
            "comments.5": {
                "_id": {"$oid": "56b8efa9f9fcee1b0000000f"},
                "body": "comment6",
                "updated_at": { "$date" : "2016-02-08T19:42:33.589Z"},
                "created_at": { "$date" : "2016-02-08T19:42:33.589Z"}
            }
        }
    }
}'''

oplog_i_01 = '''{
    "ts": "6249008760904220673",
    "t": 2,
    "h": "6498594270312035435",
    "v": 2,
    "op": "i",
    "ns": "rails4_mongoid_development.posts",
    "o": {
        "_id": {"$oid": "56b8f05cf9fcee1b00000010"},
        "title": "title3",
        "body": "body3",
        "user_id": "56b8d7caf9fcee1b00000001",
        "updated_at": { "$date" : "2016-02-08T19:45:32.501Z"},
        "created_at": { "$date" : "2016-02-08T19:45:32.501Z"}
    }
}'''

oplog_d_01 = '''{
    "ts": "6249009615602712577",
    "t": 2,
    "h": "-3308614633402865095",
    "v": 2,
    "op": "d",
    "ns": "rails4_mongoid_development.posts",
    "o": {
        "_id": {"$oid": "56b8da51f9fcee1b00000006"}
    }
}'''

oplog_u_02 = '''{
    "ts": "6249010526135779329",
    "t": 2,
    "h": "7673062055086646593",
    "v": 2,
    "op": "u",
    "ns": "rails4_mongoid_development.posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000007"}
    },
    "o": {
        "$set": {
            "updated_at": { "$date" : "2016-02-08T19:52:23.883Z"}
        }
    }
}'''

oplog_u_03 = '''{
    "ts": "6249011956359888897",
    "t": 2,
    "h": "7863054063887715514",
    "v": 2,
    "op": "u",
    "ns": "rails4_mongoid_development.posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000007"}
    },
    "o": {
        "$set": {
            "comments": [
                {
                    "_id": {"$oid": "56b8f344f9fcee1b00000018"},
                    "updated_at": { "$date" : "2016-02-08T19:57:56.678Z"},
                    "created_at": { "$date" : "2016-02-08T19:57:56.678Z"}
                }
            ]
        }
    }
}'''

oplog_u_04 = '''{
    "ts": "6249011999309561857",
    "t": 2,
    "h": "5531998682355049033",
    "v": 2,
    "op": "u",
    "ns": "rails4_mongoid_development.posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000007"}
    },
    "o": {
        "$set": {
            "comments.1": {
                "_id": {"$oid": "56b8f34ef9fcee1b00000019"},
                "body": "commments2222",
                "updated_at": { "$date" : "2016-02-08T19:58:06.008Z"},
                "created_at": { "$date" : "2016-02-08T19:58:06.008Z"}
            }
        }
    }
}'''

oplog_u_05 = '''{
    "ts": "6249012068029038593",
    "t": 2,
    "h": "8307152860560416908",
    "v": 2,
    "op": "u",
    "ns": "rails4_mongoid_development.posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000007"}
    },
    "o": {
        "$set": {
            "comments.2": {
                "_id": {"$oid": "56b8f35ef9fcee1b0000001a"},
                "updated_at": { "$date" : "2016-02-08T19:58:22.847Z"},
                "created_at": { "$date" : "2016-02-08T19:58:22.847Z"}
            }
        }
    }
}'''

oplog_u_06 = '''{
    "ts": "6249012252712632321",
    "t": 2,
    "h": "1507724081854138926",
    "v": 2,
    "op": "u",
    "ns": "rails4_mongoid_development.posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000007"}
    },
    "o": {
        "$set": {
            "comments": [
                {
                    "_id": {"$oid": "56b8f344f9fcee1b00000018"},
                    "updated_at": { "$date" :  "2016-02-08T19:57:56.678Z"},
                    "created_at": { "$date" :  "2016-02-08T19:57:56.678Z"}
                },
                {
                    "_id": {"$oid": "56b8f35ef9fcee1b0000001a"},
                    "updated_at": { "$date" : "2016-02-08T19:58:22.847Z"},
                    "created_at": { "$date" : "2016-02-08T19:58:22.847Z"}
                }
            ]
        }
    }
}'''

oplog_u_07 = '''{
    "ts": "6249012828238249985",
    "t": 2,
    "h": "6500669658302126550",
    "v": 2,
    "op": "u",
    "ns": "rails4_mongoid_development.posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000007"}
    },
    "o": {
        "$set": {
            "comments": [
                {
                    "_id": {"$oid": "56b8f344f9fcee1b00000018"},
                    "updated_at": { "$date" : "2016-02-08T19:57:56.678Z"},
                    "created_at": { "$date" : "2016-02-08T19:57:56.678Z"}
                }
            ]
        }
    }
}'''

oplog_u_08 = '''{
    "ts": "6249013055871516673",
    "t": 2,
    "h": "-3907160829967975359",
    "v": 2,
    "op": "u",
    "ns": "rails4_mongoid_development.posts",
    "o2": {
        "_id": {"$oid": "56b8f05cf9fcee1b00000010"}
    },
    "o": {
        "$set": {
            "title": "sada",
            "updated_at": { "$date" : "2016-02-08T20:02:12.985Z"}
        }
    }
}'''

oplog_u_09 = '''{
    "ts": "6249012828238249985",
    "t": 2,
    "h": "6500669658302126550",
    "v": 2,
    "op": "u",
    "ns": "rails4_mongoid_development.posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000007"}
    },
    "o": {
        "$set": {
            "comments.1._id": {"$oid": "56b8f344f9fcee1b00000018"},
            "comments.1.updated_at": "2016-02-08T19:57:56.678Z",
            "comments.1.created_at": "2016-04-08T19:57:56.678Z"
        }
    }
}'''


test_data_01 = {"aaa": {"bbb": {"ccc": {"__ddd": {"$eeee": "abcdef"}}}}}

test_data_02 = '''{
	"ts": "1465220614, 2",
	"h": "143890519546180493",
	"v": 2,
	"op": "u",
	"ns": "rails4_mongoid_development.rated_posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000010"}
    },
	"o": {
		"$set": {
			"comments.1.rates.1.items.1.approvals": [{
				"_id": {"$oid": "57557e06cf68795ee8055c77"},
				"message": {},
				"initial_comment": {},
				"message_type_id": 26,
				"updated_at": {"$data" : "2016-06-06T13:43:34.389Z"},
				"created_at": {"$data" : "2016-06-06T13:43:34.389Z"}
			}]
		}
	}
}'''


test_data_03 = '''{
	"ts": "1465220614, 2",
	"h": "143890519546180493",
	"v" : 2,
	"op" : "u",
	"ns" : "rails4_mongoid_development.rated_posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000012"}
    },
	"o" : {
		"$set" : {
			"comments.1.updated_at" : {"$date":"2016-02-08T19:58:22.847Z"},
			"comments.1.rates.1.user_id" : "B",
			"comments.1.rates.2.rate" : 2,
			"comments.1.rates.2.namesadas" : "XXX"
		}
	}
}'''

test_data_04 = '''{
	"ts": "1465220614, 2",
	"h": "143890519546180493",
	"v" : 2,
	"op" : "u",
	"ns" : "rails4_mongoid_development.rated_posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000013"}
    },
	"o" : {
		"$set" : {
			"comments.1.rates.1.user_id" : "B",
			"comments.1.rates.2.rate" : 67,
			"comments.1.rates.2.item_rates" : [
			    {
			        "_id": {"$oid": "57557e06cf68790000000000"},
			        "name": "Ivan"
			    },
			    {
			        "_id": {"$oid": "57557e06cf68790000000001"},
			        "name": "Susanin"
			    }
			]

		}
	}
}'''

test_data_05 = '''{
	"ts": "1465220614, 2",
	"h" : "-1422482527797357621",
	"v" : 2,
	"op" : "u",
	"ns" : "quote_management.rated_posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000014"}
    },
	"o" : {
		"$set" : {
			"comments.1.rates.2.user_info.name" : "Vasya"
		}
	}
}
'''

test_data_06 = '''{
	"ts": "1465220614, 2",
    "h" : "8003031459294353269",
    "v" : 2,
    "op" : "u",
    "ns" : "quote_management.rated_posts",
    "o2" : { "_id" : 503078 },
    "o" : {
        "$unset" : {
            "comments.2._id" : true
        }
    }
}
'''

test_data_07 = '''{
	"ts": "1465220614, 2",
    "h" : "8003031459294353269",
    "v" : 2,
    "op" : "u",
    "ns" : "quote_management.rated_posts",
    "o2" : { "_id" : 503078 },
    "o" : {
        "$unset" : {
            "comments.2.rates.8.user_info" : true
        }
    }
}
'''

test_data_08 = '''{
	"ts": "1465220614, 2",
    "h" : "8003031459294353269",
    "v" : 2,
    "op" : "u",
    "ns" : "quote_management.rated_posts",
    "o2" : { "_id" : 503078 },
    "o" : {
        "$unset" : {
            "comments.2.rates.9.item_rates" : true
        }
    }
}
'''

test_data_09 = '''{
	"ts": "1465220614, 2",
    "h" : "8003031459294353269",
    "v" : 2,
    "op" : "u",
    "ns" : "quote_management.rated_posts",
    "o2" : { "_id" : 503078 },
    "o" : {
        "$set" : {
            "comments.2.tests.5" : 24
        }
    }
}
'''
test_data_10 = '''{
	"ts": "1465220614, 2",
    "h" : "8003031459294353269",
    "v" : 2,
    "op" : "u",
    "ns" : "quote_management.rated_posts",
    "o2" : { "_id" : 503078 },
    "o" : {
        "$unset" : {
            "comments.2.tests" : true
        }
    }
}
'''

test_data_11 = '''{
	"ts": "1465220614, 2",
    "h" : "8003031459294353269",
    "v" : 2,
    "op" : "u",
    "ns" : "quote_management.rated_posts",
    "o2" : { "_id" : 503078 },
    "o" : {
        "$unset" : {
            "_id" : true
        }
    }
}
'''

# test_data_12 = '''{
# 	"ts": "1465220614, 2",
#     "h" : "8003031459294353269",
#     "v" : 2,
#     "op" : "u",
#     "ns" : "quote_management.provisioning_states",
#     "o2": {
#         "_id": {"$oid": "111111111111111111111111"}
#     },
# 	"o" : {
# 	"$unset" : {
# 			"_slugs" : true
# 		}
# 	}
# }
# '''


test_data_13 = '''{
	"ts": "1465220614, 2",
	"h" : "-1422482527797357621",
	"v" : 2,
	"op" : "u",
	"ns" : "quote_management.rated_posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000014"}
    },
	"o" : {
		"$set" : {
			"title" : "Glory For Ukraine",
			"body" : "Glory For Heroes",
			"rates":[
			    {
    			 	"_id": {
	    	            "oid": "aaaaaaaaaaaaassssssssssasdas",
		                "bsontype": 7
	                },
		            "user_id" : "444444rrwerr34r",
		            "rate" : 555
	            },
			    {
    			 	"_id": {
	    	            "oid": "aaaaaaaaaaaaasasdsadasdasdasd",
		                "bsontype": 7
	                },
		            "user_id" : "987987978979",
		            "rate" : 7777
	            }
	        ]
		}
	}
}
'''
