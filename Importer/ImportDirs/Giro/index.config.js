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
             }
          },
          "analyzer" : {
             "lc_keyword" : {
                "tokenizer" : "keyword",
                "filter": ["lowercase", "asciifolding"],
                alias: ["default_search", "default_index"]
             },
              "lc_text" : {
                 "char_filter" : ["html_strip", "repl_dot"],
                 "tokenizer" : "standard",
                 "filter": ["lowercase", "asciifolding"]
              },
             "lc_date_text" : {
                "char_filter" : ["date_totext"],
                "tokenizer" : "standard",
                "filter": "date_removetime"
             }
          }
      }
   },
   mappings: {
         doc: {
            _meta: { lastmod:""}, 
            properties: { 
               "date": {"type": "date", "index": "not_analyzed"},
               "name": {"type": "string", "analyzer": "lc_text", "include_in_all": false},
               "account": {"type": "string", "analyzer": "lc_text", "include_in_all": false},
               "account_other": {"type": "string", "analyzer": "lc_text", "include_in_all": false},
               "type": {"type": "string", "analyzer": "lc_text", "include_in_all": false},
               "mutation_code": {"type": "string", "analyzer": "lc_text", "include_in_all": false},
               "amount": {"type": "double", "index": "not_analyzed"},
               "mutation": {"type": "string", "analyzer": "lc_text", "include_in_all": false},
               "comment": {"type": "string", "analyzer": "lc_text", "include_in_all": false}
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