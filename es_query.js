'use strict';
module.exports = {
    "fields": [
       "created_at"
    ], 
    "query": {
        "match_all": {}
    }, 
    "sort": [
       {
          "created_at": {
             "order": "asc"
          }
       }
    ]
};
/*module.exports = {
    "query": {
        "match_all": {}
    }
};
*/