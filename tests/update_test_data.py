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

oplog_u_02_A = '''{
    "ts": "6249010526135779329",
    "t": 2,
    "h": "7673062055086646593",
    "v": 2,
    "op": "u",
    "ns": "rails4_mongoid_development.rated_posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000007"}
    },
    "o": {
        "$set": {
            "number": 254.4
        }
    }
}'''

oplog_u_02_B = '''{
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
			"comments.1.rates.2.rate" : "2",
			"comments.1.rates.2.namesadas" : "XXX"
		}
	}
}'''

test_data_03_A = '''{
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
			"comments.1.rates.2.rate" : "67",
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
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000014"}
    },
    "o" : {
        "$unset" : {
            "comments.2._id" : true
        }
    }
}
'''

test_data_06_A = '''{
	"ts": "1465220614, 2",
    "h" : "8003031459294353269",
    "v" : 2,
    "op" : "u",
    "ns" : "quote_management.rated_posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000015"}
    },
    "o" : {
        "$unset" : {
            "enclosed" : true
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
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000015"}
    },
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
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000015"}
    },
    "o" : {
        "$unset" : {
            "comments.2.rates.9.item_rates" : true
        }
    }
}
'''

test_data_08_all ='''{
	"ts": "1465220614, 2",
    "h" : "8003031459294353269",
    "v" : 2,
    "op" : "u",
    "ns" : "quote_management.rated_posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000015"}
    },
    "o" : {
        "$unset" : {
            "comments.2.rates.9.item_rates" : true,
            "comments.2.rates.8.user_info" : true,
            "enclosed" : true,
            "comments.2._id" : true
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
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000013"}
    },
    "o" : {
        "$set" : {
            "comments.2.tests.5" : 24
        }
    }
}
'''
#    "o2" : { "_id" : 503078 },

test_data_09_A = '''{
	"ts": "1465220614, 2",
    "h" : "8003031459294353269",
    "v" : 2,
    "op" : "u",
    "ns" : "quote_management.rated_posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000013"}
    },
    "o" : {
        "$set" : {
            "comments.2.tests.5" : 24.7
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
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000015"}
    },
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
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000015"}
    },
    "o" : {
        "$unset" : {
            "_id" : true
        }
    }
}
'''

test_data_12 = '''{
	"ts": "1465220614, 2",
    "h" : "8003031459294353269",
    "v" : 2,
    "op" : "u",
    "ns" : "quote_management.rated_posts",
    "o2": {
        "_id": {"$oid": "111111111111111111111111"}
    },
	"o" : {
	"$unset" : {
			"tests" : true
		}
	}
}
'''


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

test_data_14 = '''{
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
		    "body":"SOME text",
            "number": 33,
            "tests": [123, 4 ,8],
			"comments.1.rates.1.user_id" : "B",
			"comments.1.rates.2.rate" : "67",
			"comments.1.rates.2.item_rates" : [
			    {
			        "_id": {"$oid": "57557e06cf68790000000000"},
			        "name": "Ivan"
			    },
			    {
			        "_id": {"$oid": "57557e06cf68790000000001"},
			        "name": "Susanin"
			    }
			],
			"comments.1.rates.2.item_ratessa" : [
			    {
			        "_id": {"$oid": "57557e06cf68790000000000"},
			        "name": "Ivan"
			    },
			    {
			        "_id": {"$oid": "57557e06cf68790000000001"},
			        "name": "Susanin"
			    }
			],
			"enclosed":{"field_array":["234", "ertret"]}
		}
	}
}'''



test_data_15 = '''{
	"ts": "1465220614, 2",
    "h" : "8003031459294353269",
    "v" : 2,
    "op" : "u",
    "ns" : "quote_management.rated_posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000013"}
    },
	"o" : {
		"$set" : {
			"enclosed" : {
				"_id" : {"$oid":"57640cb0cf6879b3fcf0d3f6"},
				"field1" : "marty mackfly",
				"field2" : 300,
				"field_array" : []
			}
		}
	}
}
'''

test_data_15_A = '''{
	"ts": "1465220614, 2",
    "h" : "8003031459294353269",
    "v" : 2,
    "op" : "u",
    "ns" : "quote_management.rated_posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000013"}
    },
	"o" : {
		"$set" : {
			"enclosed" : {
				"_id" : {"$oid":"57640cb0cf6879b3fcf0d3f6"},
				"field1" : "marty mackfly",
				"field2" : 300,
				"field_array" : null,
				"not_in_schema_array":[]
			}
		}
	}
}
'''

test_data_16 = '''{
	"ts": "1465220614, 2",
    "h" : "8003031459294353269",
    "v" : 2,
    "op" : "u",
    "ns" : "quote_management.rated_posts",
    "o2": {
        "_id": {"$oid": "56b8da59f9fcee1b00000013"}
    },
	"o" : {
		"$set" : {"enclosed":{
			"WRONGmanager" : null,
			"WRONGdisplay_name" : null,
			"WRONGemail" : null,
			"WRONGldap_group_memberships" : null,
			"WRONGcountry_code" : null,
			"WRONGupdated_at" : {"$date":"2016-06-17T14:41:57.705Z"}
		}}
	}
}
'''

test_data_17 = '''{
	"ts": "1465220614, 2",
    "h" : "8003031459294353269",
    "v" : 2,
    "op" : "u",
    "ns" : "quote_management.rated_posts",
    "o2": {"_id": {"$oid": "56b8da59f9fcee1b00000013"}},
	"o" : {
		"$set" : {
			"WRONGmanager" : null,
			"WRONGdisplay_name" : null,
			"WRONGemail" : null,
			"WRONGldap_group_memberships" : null,
			"WRONGcountry_code" : null,
			"WRONGupdated_at" : {"$date":"2016-06-17T14:41:57.705Z"}
		}
	}
}
'''


test_data_18 = '''{
   "ts": "1465220614, 2",
   "h": "-1447958161854102698",
   "v": 2,
   "op": "u",
   "ns": "rails4_mongoid_development.rated_posts",
   "o2": {"_id": {"$oid": "56b8da59f9fcee1b00000013"}},
   "o": {
       "$set": {
           "rates.1": {
               "_id": {"$oid":"5769a855cf6879d0b0a01978"},
               "name": "Configuration 2",
               "a_filed_with_id": {"$oid":"5769a7ebcf6879d0b0a01971"},
               "another_filed_with_id2":{
                    "onemore_enclosed_level": {
                        "$oid":"5769a7ebcf6879d0b0a01973",
                        "bsontype":7
                    },
                    "some_strange_field":45
                },
               "datacenter_id": 3,
               "position": 2,
               "updated_at": {"$date":"2016-06-21T20:49:24.995Z"},
               "created_at": {"$date":"2016-06-21T20:49:24.995Z"}

           }
       }
   }
}'''



test_data_19 = '''{
	"ts" : "1465220614, 2",
	"h" : "279648261777343585",
	"v" : 2,
	"op" : "u",
	"ns" : "rails4_mongoid_development.rated_posts",
	"o2" : {"_id" : {"$oid":"54f9d91b70722d95c4ce0600"}
	},
	"o" : {
		"$set" : {
			"tests" : false,
			"body" : "test body"
		}
	}
}'''


test_data_20 = '''{
	"ts" : "1474458400, 27",
	"h" : "-5725172150357890103",
	"v" : 2,
	"op" : "u",
	"ns" : "quote_management.rated_posts",
	"o2" : {
	"_id" : {"$oid":"57e26d602356e3b6c8ad7e5d"}
	},
	"o" : {
		"$set" : {
			"enclosed" : {}
		}
	}
}
'''

test_data_20_A = '''{
	"ts" : "1474458400, 27",
	"h" : "-5725172150357890103",
	"v" : 2,
	"op" : "u",
	"ns" : "quote_management.rated_posts",
	"o2" : {
	"_id" : {"$oid":"57e26d602356e3b6c8ad7e5d"}
	},
	"o" : {
		"$set" : {
			"enclosed" : null
		}
	}
}
'''

test_data_21__ = '''{
   "ts": "1465220614, 2",
   "h": "-1447958161854102698",
   "v": 2,
   "op": "u",
   "ns": "rails4_mongoid_development.rated_posts",
   "o2": {"_id": {"$oid": "56b8da59f9fcee1b00000013"}},
   "o": {
       "$set": {
           "test_bigint":1234,
           "rates.1": {
               "_id": {"$oid":"5769a855cf6879d0b0a01978"},
               "name": "Configuration 2",
               "a_filed_with_id": {"$oid":"5769a7ebcf6879d0b0a01971"},
               "another_filed_with_id2":{
                    "onemore_enclosed_level": {
                        "$oid":"5769a7ebcf6879d0b0a01973",
                        "bsontype":7
                    },
                    "some_strange_field":{"Struct not in schema":"value not in schema"}
                },
               "datacenter_id": 3,
               "position": 2
           }
       }
   }
}'''

test_data_22 = '''{
   "ts": "1465220614, 2",
   "h": "-1447958161854102698",
   "v": 2,
   "op": "u",
   "ns": "rails4_mongoid_development.rated_posts",
   "o2": {"_id": {"$oid": "56b8da59f9fcee1b00000013"}},
   "o": {
       "$set": {
            "comments.10.rates.3":{
                "rate":"24",
                "user_info":{}
            }
        }
   }
}'''
