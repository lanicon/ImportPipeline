﻿{
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
         _id: {path: "virtualFilename"},
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
            content_type: {"type": "string", "index": "no"},   
            content_type2: {"type": "string", "index": "no"},   
            fileDate: {"type": "date", "index": "no"},   
            filename: { 
               "type": "string",
               "index": "no"
            },
            virtualFilename: { 
               "type": "string",
               "analyzer": "lc_text"
            },
            "message-to": {
               "type": "string",
               "analyzer": "lc_text"
            },
            "message-cc": {
               "type": "string",
               "analyzer": "lc_text"
            },
            "message-from": {
               "type": "string",
               "analyzer": "lc_text"
            },
            filefacets: {
               type: "string", 
               analyzer: "lc_keyword"
            },
            shortcontent: {
               "type": "string", index:"no", "store": "yes" , "analyzer": "lc_text"    
            },
            content: {
               "type": "string",
               "analyzer": "lc_text"
            },
            title: {
               "type": "string",
               "analyzer": "lc_text"
            },
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
            keywords: {
               "type": "multi_field",
               fields: {
                  keywords: {type: "string", "analyzer": "lc_text"},
                  facet: {type: "string", analyzer: "lc_keyword"}
               }
            },
            "page_count": {
               "type": "long"
            }
         }
      },
      admin_: {
            _meta: { lastmod:""}, 
            properties: { 
               name: { "type" : "string", "index":"no", "store": "yes" },
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