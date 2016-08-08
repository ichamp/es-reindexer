//index_ids_batch.js

'use strict';

var util = require('util');
var elasticsearch = require('elasticsearch');
var async = require('async');

var CONFIG = require('./config.js');

var esOps = require('./es_ops');

var esFrom = new elasticsearch.Client({
	host: CONFIG.FROM.ES_URL,
	requestTimeout: CONFIG.FROM.TOUT
});

var esTo = new elasticsearch.Client({
	host: CONFIG.TO.ES_URL,
	requestTimeout: CONFIG.FROM.TOUT
});

var globalCounter = 0;
var pushedCount = 0;
var failedCount = 0;
var informOnce = 0;
var processedTillId = 0;
var failedSearchBatchCount = 0;

var BULK_AR = [];


var indexFrom = {
	index: CONFIG.FROM.ES_INDEX,
	type: CONFIG.FROM.ES_TYPE
};

var indexTo = {
	index: CONFIG.TO.ES_INDEX,
	type: CONFIG.TO.ES_TYPE
};

function GetAndPush(start, end) {
	//searchSingle: function (client, esIndex, searchObj, cb); 
	//console.log('START = ' + start + '  END = ' + end);
	var size = (end - start) + 1;

	var query = {
		"size": size,
		"query": {
			"range": {
				"id": {
					"from": start,
					"to": end
				}
			}
		}
	};

	//console.log(JSON.stringify(query));

	esOps.searchSingle(esFrom, indexFrom, query, function(err, res) {
		if (err) {
			util.log('Error SEARCH FROM = ' + start + '  TO = ' + end);
			failedSearchBatchCount++;
			cb();
		} else {

			//console.log('Formatting data below to form bulk insert request');
			var bulkAr = transform(res);
			if (bulkAr.length != 0) {
				esOps.dumpESBulk(esTo, bulkAr, function(err, res) {
					if (err) {
						util.log('Error DUMP FROM = ' + start + '  TO = ' + end);
						failedCount += bulkAr.length;
						cb();
					} else {
						pushedCount += bulkAr.length;
						processedTillId = end;
						cb();
					}
				});
				//console.log(JSON.stringify(bulkAr));
			} else {
				cb();
			}
		}
	});
}

//GetAndPush(1,1000,function(err,res){});

var G_START = 1;
var G_END = 1000;

var G_SIZE = 1000;

var G_LIMIT_END = 62476683;

var once = false;

function cb() {

	showSomeProgress();

	if (once == false) {
		once = true;
		G_START = 1;
		G_END = 1000;
	} else {
		G_START += G_SIZE;
		G_END += G_SIZE;
	}

	if (G_START > G_LIMIT_END)
		process.exit(1);
	else
		GetAndPush(G_START, G_END);
}


//Start execustion over here
//cb();


var aimDiff = 10;
var aim = 10;

function transform(jsonAr) {

	var bulk = [];

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
		meta.index._id = json.id;

		bulk.push(meta);
		bulk.push(json);

	});
	//console.log(bulk);
	return bulk;
}

function showSomeProgress() {
//console.log('entered showSomeProgress');
	if (pushedCount + failedCount >= aim) {
		aim = aim + aimDiff;
		++globalCounter;
		//console.log('Hey Sid');
		util.log('GC = ' + globalCounter + '  Pushed = ' + pushedCount + ' failed = ' + failedCount + ' failedSBCount = ' + failedSearchBatchCount + ' processedTillId = ' + processedTillId);
	}
}


//IEF
(function() {
  if (require.main === module) {

    aim = aimDiff = Number(process.argv[2]) || 10000;	//reporting after this number of docs.

    G_START = Number(process.argv[3]) || 1;		//starting PID
    G_SIZE = Number(process.argv[4]) || 1000;	//index ids searched in chunks of this
    G_END = (G_START + G_SIZE) - 1;				
    G_LIMIT_END = Number(process.argv[5]) || 63476683;	//end PID as per DB

    util.log('Will report after every ' + aim + ' docs pushed or failed');
    cb();
  }
}());
