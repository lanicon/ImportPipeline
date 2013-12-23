﻿{
      "number_of_shards" : 5,
      "number_of_replicas" : 0,
   settings: {  
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
               id: {
                  "type": "string",
                  "index": "not_analyzed"   
               },
               "name": {
                  "type": "string",
                  "analyzer": "lc_keyword",
                  "include_in_all": false,
                  "omit_norms": true
               },
               "code": {
                  "type": "string",
                  "index": "no"
               },
               "price": {
                  "type": "double",
                  "index": "no"
               },
               "pos3m":  {"type": "double"},
               "pos6m":  {"type": "double"},
               "pos12m": {"type": "double"},
               "pos36m": {"type": "double"},
               "dev3m":  {"type": "double"},
               "dev6m":  {"type": "double"},
               "dev12m": {"type": "double"},
               "dev36m": {"type": "double"},
               "priceOpened": {
                  "type": "double",
                  "index": "no"
               },
               "date": {
                  "type": "date",
                  "format": "dateOptionalTime"
               },
               "history": {
                  "properties": {
                     "price": {"type": "double", "index": "no"},
                     "date": {"type": "date", "format": "dateOptionalTime", "index": "no"}
                  }
               }   
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