﻿{
   settings: {  
      "number_of_shards" : 1,
      "number_of_replicas" : 0,
      "analysis" : {
         "char_filter" : {
            "html_strip" : {
               "type" : "html_strip",
               "read_ahead" : 1024
            },
            "preparekw1" : {
               "type" : "mapping",
               "mappings" : [",=>\\u0020"]
            },
            "preparekw2" : {
               "type" : "mapping",
               "mappings" : ["\\u0020\\u0020=>\\u0020"]
            },
            "repl_dot" : {
               "type" : "mapping",
               "mappings" : [".=>\\u0020", "\\uFFFD=>\\u0020"]
            },
            "date_totext" : {
               "type" : "mapping",
               "mappings" : ["-01-=>-jan januari-",
                             "-02-=>-feb februari-",
                             "-03-=>-mrt maart-",
                             "-04-=>-apr april-",
                             "-05-=>-mei-",
                             "-06-=>-jun juni-",
                             "-07-=>-jul juli-",
                             "-08-=>-aug augustus-",
                             "-09-=>-sep september-",
                             "-10-=>-okt oktober-",
                             "-11-=>-nov november-",
                             "-12-=>-dec december-",
                             ":=> "
               ]
            }
         },
          "filter" : {
               diacrit: {
                   type: "asciifolding"
               },
             "date_removetime": {
                   type: "pattern_replace",
                   "pattern": "[tT].*[zZ]",
                   "replacement": ""
             },
             "letter_digit": {
                   type: "pattern_replace",
                   "pattern": "[^\\p{L}\\p{N}]",
                   "replacement": ""
             },
             "stop" : {
                "type" : "stop",
                "stopwords" : ["test-stop"]
             },
              "stop2" : {
                 "type" : "stop",
                 "stopwords" : ["stop2-1", "stop2-2"]
              }
          },
          "analyzer" : {
             "lc_keyword" : {
                "tokenizer" : "keyword",
                filter: ["diacrit", "lowercase"],
                alias: ["default_search", "default_index"]
             },
              "lc_text" : {
                 "char_filter" : ["html_strip", "repl_dot"],
                 "tokenizer" : "standard",
                 filter: ["diacrit", "lowercase"]
              },
             "lc_date_text" : {
                "char_filter" : ["date_totext"],
                "tokenizer" : "standard",
                filter: "date_removetime"
             }
          }
      }
   },
   mappings: {
         doc: {
            _meta: { lastmod:""}, 
            _id: {path: "id"},
            "dynamic_templates" : [ 
               { 
                  "template_1" : { 
                     "path_match" : "*",
                     "match_mapping_type" : "string",
                     "mapping" : { 
                        "type" : "string",
                        "index" : "not_analyzed"
                     }
                  }
               }
            ],

            properties: { 
               "id": {"type": "string", "index" : "no"},
               "iso2": {"type": "string", "analyzer" : "lc_keyword"},
               "iso3": {"type": "string", "analyzer": "lc_keyword"},
               "isoNum": {"type": "string", "index" : "not_analyzed"},
               "fips": {"type": "string", "analyzer" : "lc_keyword"},
               "name": {"type": "string", "analyzer" : "lc_text"},
               "capital": {"type": "string", "analyzer" : "lc_text"},
               "area": {"type": "string", "index" : "not_analyzed"},
               "population": {"type": "string", "index" : "not_analyzed"},
               "continent": {"type": "string", "analyzer" : "lc_keyword"},
               "tld": {"type": "string", "analyzer" : "lc_keyword"},
               "curCode": {"type": "string", "analyzer" : "lc_keyword"},
               "curName": {"type": "string", "analyzer" : "lc_text"},
               "phone": {"type": "string", "analyzer" : "lc_text"},
               "postalCode": {"type": "string", "analyzer" : "lc_keyword"},
               "postalRegex": {"type": "string", "analyzer" : "lc_keyword"},
               "lang": {"type": "string", "analyzer" : "lc_text"},
               "neighbors": {"type": "string", "analyzer" : "lc_text"},
               "altFips": {"type": "string", "analyzer" : "lc_keyword"}
            }
      },
      
      
      admin_: {
            _meta: { lastmod:""}, 
            properties: { 
               name_: { "type" : "string", "index":"no", "store": "yes" },
               lastUpdUtc: { "type" : "date", "index":"no", "store": "yes" }
            }
      },
      errors_: {
            properties: { 
               errorFileName: { "type" : "string", "analyzer": "lc_text", "store": "yes" },
               errorDate:  { "type" : "date", "index":"no", "store": "yes" },
               errorText: { "type" : "string", "index":"no", "store": "yes" },
               errorStack: { "type" : "string", "index":"no", "store": "yes" }
            }
      }
   }
}