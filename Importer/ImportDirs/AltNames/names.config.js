{
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
            "umlaut": {
               "type" : "mapping",
               "mappings" : ["ae=>a", "oe=>o", "ue=>u"]
            },
            "apostrophe_to_blank": {
              "type":  "pattern_replace",
              "pattern": "[\\u02b9-\\u02df,\\u02e4-\\u02ff]", "replacement": " " 
            },
            "remove_soundmarks": {
              "type":  "pattern_replace",
              "pattern": "[\\u30fc,\\uff70,\\uff9e]", "replacement": "" 
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
                filter: ["lowercase", "asciifolding"],
                alias: ["default_search", "default_index"]
             },
              "lc_text" : {
                 "char_filter" : ["html_strip", "umlaut", "apostrophe_to_blank", "remove_soundmarks", "repl_dot"],
                 "tokenizer" : "standard",
                 filter: ["lowercase", "asciifolding"]
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
               "nameId": {"type": "string", "index" : "not_analyzed"},
               "name": {"type": "string", "analyzer": "lc_text"},
               "lat": {"type": "string", "index" : "not_analyzed"},
               "lon": {"type": "string", "index" : "not_analyzed"},
               "countryCode": {"type": "string", "index" : "not_analyzed"},
               "featureClass": {"type": "string", "index" : "not_analyzed"},
               "featureCode": {"type": "string", "index" : "not_analyzed"},
               "altCountryCode": {"type": "string", "index" : "not_analyzed"},
               "altNames": {"type": "string", "analyzer" : "lc_text"},
               "admin1": {"type": "string", "index" : "not_analyzed"},
               "admin2": {"type": "string", "index" : "not_analyzed"},
               "admin3": {"type": "string", "index" : "not_analyzed"},
               "admin4": {"type": "string", "index" : "not_analyzed"},
               "population": {"type": "string", "index" : "not_analyzed"},
               "elevation": {"type": "string", "index" : "not_analyzed"},
               "dem": {"type": "string", "index" : "not_analyzed"},
               "timezone": {"type": "string", "index" : "not_analyzed"},
               "lastUpdate": {"type": "string", "index" : "not_analyzed"}
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