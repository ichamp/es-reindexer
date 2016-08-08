'use strict';

/***** 
This script publishes to RabbitMq, the ids to be indexed on the basis of scan and scroll API.
The ids are fetched as per search query mentioned in the file "query_index_search.js"

    var bulksize = process.argv[2] || 1000;
    var sleepTimeMs = process.argv[3] || 2000;
    aim = aimDiff = process.argv[4] || 10000;
    var numShards = process.argv[5] || 10;

node scan_scroll_indexing <bulkSize> <sleepTimeMS> <aimDiff> <numShards>

bulkSize -> bulkSize to fetch records from ES in one scroll.
sleepTimeMs -> sleep duration before calling next scroll
aimDiff -> will report running status after this much count of docs has been processed iteratively
numShards -> scroll API fetches bulkSize basis number of shards.
******/

var util = require('util');
var elasticsearch = require('elasticsearch');

var ESQUERY = require('./es_query');
var CONFIG = require('./config.js');

var esFrom = new elasticsearch.Client({
  host: CONFIG.FROM.ES_URL,
  requestTimeout: CONFIG.FROM.TOUT
});

var esTo = new elasticsearch.Client({
  host: CONFIG.TO.ES_URL,
  requestTimeout: CONFIG.FROM.TOUT
});

console.log('Printing ES query used for reindexing below');
console.log(JSON.stringify(ESQUERY));

var globalCounter = 0;
var pushedCount = 0;
var failedCount = 0;
var informOnce = 0;


var PUSH_TO_RQUEUE = true;

var BULK_AR = [];

var CATALOG_SEARCH_INDEX = {
  index: "catalog_v2",
  type: "refiner"
};


function GetAndPush(sizePerShard, sleepTimeMs) {
  esFrom.search({
    index: CONFIG.FROM.ES_INDEX,
    type: CONFIG.FROM.ES_TYPE,
    scroll: '1m',
    search_type: 'scan',
    body: {
      "size": sizePerShard,
      "query": ESQUERY.query
    }
  }, function getMoreUntilDone(error, response) {

    function ProcessNextBatchLocal() {
      if (response.hits.total > pushedCount + failedCount) {
        esFrom.scroll({
          scrollId: response._scroll_id,
          scroll: '1m'
        }, getMoreUntilDone);
      } else {
        util.log('PROCESSED everything STOP PROCESS');
        util.log('globalCount = ' + globalCounter + '  PushedCount = ' + pushedCount + ' failedCount = ' + failedCount);
        process.exit(0);
      }

    }

    if (informOnce++ === 0) {
      util.log('Total entries in ES to be processed = ' + response.hits.total);
      var myVarLocal = setTimeout(ProcessNextBatchLocal, sleepTimeMs);
      return;
    }

    if (error) {
      util.log('Some error in running fetching docs from search query');
      util.log(error);
    }

    if (!response && !response.hits && !response.hits.hits) {
      util.log('Response not properly received from ES');
    }

    //console.log('Sid printing hits below');
    //console.log(response);
    //console.log(response.hits.hits.length);

    // response.hits.hits.forEach(function(hit){

    // });


    BULK_AR = transform(response.hits.hits);

    dumpESBulk(esTo, BULK_AR, function(err, res) {
      if (err) {
        console.log('Some error with bulk');
        console.log(err);

        //Log the data in a file
        failedCount += response.hits.hits.length;
        showSomeProgress();
        ProcessNextBatchLocal();
      } else {
        //console.log('Bulk insert successful');
        pushedCount += response.hits.hits.length;
        showSomeProgress();
        ProcessNextBatchLocal();
      }
    });

    /*
        response.hits.hits.forEach(function(hit) {
          if (PUSH_TO_RQUEUE) {
            if (Number(hit.fields[FIELDS[0]][0])) {
              util.log('#### ' +  hit.fields[FIELDS[0]][0]);
              pb.publish({
                id: hit.fields[FIELDS[0]][0]
              });
              pushedCount++;
            } else { 
              util.log('Failed to index => ' + hit.fields[FIELDS[0]][0]);
              failedCount++;
            }

          }
        });
    */
    
    //showSomeProgress();
    //var myVar = setTimeout(ProcessNextBatchLocal, sleepTimeMs);
  });

}

var aimDiff;
var aim;

function transform(jsonAr) {

  var bulk = [];

  /*
  { "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }
  { "field1" : "value1" }
  */

  var meta = {
    'index': {
      '_index': '',
      '_type': '',
      '_id': ''
    }
  };

  //console.log('Length is' + jsonAr.length);

  jsonAr.forEach(function(json) {

    var meta = {
      'index': {
        '_index': '',
        '_type': '',
        '_id': ''
      }
    };

    //console.log('Log once');
    meta.index._index = CONFIG.TO.ES_INDEX; //json._index;
    meta.index._type = CONFIG.TO.ES_TYPE; //json._type;
    meta.index._id = json._id;

    bulk.push(meta);
    bulk.push(json._source);

  });
  //console.log(bulk);
  return bulk;
}

function dumpESBulk(esClient, bulkAr, cb) {
  esClient.bulk({
    body: bulkAr
  }).then(function(resp) {
      cb(null);
    },
    function(err) {
      cb(err);
    });
}

function showSomeProgress() {

  if (pushedCount + failedCount >= aim) {
    aim = aim + aimDiff;
    ++globalCounter;
    util.log('globalCount = ' + globalCounter + '  PushedCount = ' + pushedCount + ' failedCount = ' + failedCount);
  }
}

//IEF
(function() {
  if (require.main === module) {

    var bulkSize = process.argv[2] || 1000;
    var sleepTimeMs = process.argv[3] || 2000;
    aim = aimDiff = Number(process.argv[4]) || 10000;
    var numShards = process.argv[5] || 1;

    var sizePerShard = bulkSize / numShards;
    util.log('Will report after every ' + aim + ' docs');
    GetAndPush(sizePerShard, sleepTimeMs);
  }
}());