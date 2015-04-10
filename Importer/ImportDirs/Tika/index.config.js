{
   settings: {  
      "number_of_shards" : 2,
      "number_of_replicas" : 0,
      "refresh_interval": "-1",
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
             }
          },
          "analyzer" : {
             "lc_keyword" : {
                "tokenizer" : "keyword",
                filter: ["asciifolding", "lowercase"],
                alias: ["default_search", "default_index"]
             },
              "lc_text" : {
                 "char_filter" : ["html_strip", "repl_dot"],
                 "tokenizer" : "standard",
                 filter: ["asciifolding", "lowercase"]
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
      bosdoc: {
         _meta: { lastmod:""}, 
         "_all" : {"enabled" : false},
         "dynamic_templates" : [ 
            { 
               "template_1" : { 
                  "path_match" : "*",
                  "match_mapping_type" : "string",
                  "mapping" : { 
                     "type" : "string",
                     "index" : "no"
                  }
               }
            }
         ],

         properties: { 
            content_type: {"type": "string", "index": "no"},   
            content_type2: {"type": "string", "index": "no"},   
            fileDate: {"type": "date", "index": "no", "store":"no"},   
            filename: {"type": "string", "analyzer": "lc_keyword"},
            virtualFilename: { 
               "type": "string",
               "analyzer": "lc_text"
            },
            secgroups: {"type": "string", "analyzer": "lc_keyword"},
            "recip": {"type": "string", "analyzer": "lc_text"},
            "recip_cc": {"type": "string", "analyzer": "lc_text"},
            "msg_id": {"type": "string", "analyzer": "lc_keyword"},
            "msg_ref": {"type": "string", "analyzer": "lc_keyword"},
            filefacets: {
               type: "string", 
               analyzer: "lc_keyword"
            },
            shortcontent: {
               "type": "string", index:"no" , "analyzer": "lc_text"    
            },
            numattachments: {"type": "integer"},
            numparts: {"type": "integer"},
            filesize: {"type": "integer"},
            head: {"type": "string", "index": "no"},
            content: {"type": "string", "analyzer": "lc_text"},
            title: {"type": "string", "analyzer": "lc_text"},
            subject: {"type": "string", "analyzer": "lc_text"},
            sort_subject: {"type": "string", "analyzer": "lc_keyword"},
            author: {
               "type": "multi_field",
               fields: {
                  author: {type: "string", "analyzer": "lc_text"},  
                  facet: {type: "string", analyzer: "lc_keyword"}
               }
            },
            sortauthor: {
               type: "string",
               analyzer: "lc_keyword"
            },
            doc_cat: {"type" : "string", analyzer: "lc_keyword"},
            keywords: {
               "type": "multi_field",
               fields: {
                  keywords: {type: "string", "analyzer": "lc_text"},
                  facet: {type: "string", analyzer: "lc_keyword"}
               }
            },
            "page_count": {"type": "integer"},
            "date_created": {"type": "date", "index": "no"},
            "date_modified": {"type": "date", "doc_values": true},
            "year_modified": {"type": "integer"}
         }
      },
      admin_: {
         _meta: { lastmod:""}, 
         "_all" : {"enabled" : false},
         properties: { 
               name: { "type" : "string", "index":"no"},
               lastUpdUtc: { "type" : "date", "index":"no" }
            }
      },
      errors_: {
         "_all" : {"enabled" : false},
         properties: { 
            errorFileName: { "type" : "string", "analyzer": "lc_text"},
            errorDate:  { "type" : "date", "index":"no"},
            errorText: { "type" : "string", "index":"no"},
            errorStack: { "type" : "string", "index":"no"}
         }
      }
   }
}