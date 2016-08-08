'use strict';

var debug = require('debug')('es');
var elasticsearch = require('elasticsearch');
var async = require('async');

var es_ops = {

  /*****
  Use below connect function to create connection with ES cluster.
  *****/
  connect: function (host, log) {
    var config = {
      host: host
    };

    if (log) {
      config.log = 'trace';
    }

    return new elasticsearch.Client(config);
  },

  /*****
  Use below to create a new index. It is assumed that you don't know the id of this document,
  and hence finally several copies of similar data can be there with different 'id'
  *****/
  insert: function (client, esIndex, dumpObj, cb) {
    client.create({
      index: esIndex.index,
      type: esIndex.type,
      body: dumpObj
    }).then(function (resp) {
      return cb(null, resp._id);
    }, function (err) {
      return cb(err);
    });
  },

  /*****
  Use below to create or update a document, it will inform the version of the document bumped up.
  Important is to be able to define a unique primary 'id' which is used to create or update the
  document contents
  *****/
  insertUpdateExisting: function (client, esIndex, id, dumpObj, cb) {
    client.index({
      index: esIndex.index,
      type: esIndex.type,
      id: id,
      body: dumpObj
    }).then(function (resp) {
      if (resp._id && resp._version) {
        cb(null, resp._id, resp._version);
      } else {
        cb(null, null, null);
      }
    }, function (err) {
      cb(err);
    });
  },

  count: function (client, esIndex, searchObj, cb) {
    client.count({
      index: esIndex.index,
      type: esIndex.type,
      //body: searchObj
    }).then(function (resp) {
      if (resp) {
        cb(null, resp.count);
      } else {
        cb(new Error('Count not exists. Error with count query'));
      }
    }, function (err) {
      cb(err);
    });
  },

  dumpESBulk: function(client, bulkAr, cb) {
    client.bulk({
      body: bulkAr
    }).then(function(resp) {
        cb(null);
      },
      function(err) {
        cb(err);
      });
  },

  searchSingle: function (client, esIndex, searchObj, cb) {
    client.search({
      index: esIndex.index,
      type: esIndex.type,
      body: searchObj
    }).then(function (resp) {
      var result = [];
      if (resp && resp.hits && resp.hits.hits && resp.hits.hits.length) {
        result = resp.hits.hits.map(function (e) {
          return e._source;
        });
      }
      cb(null, result);
    }, function (err) {
      cb(err);
    });
  },

  getQuery: function (options) {
    var body = {
      query: {
        bool: {
          must: []
        }
      }
    };

    if (options) {
      if (options.size) {
        body.size = options.size;
      }

      if (options.match) {
        var matchFields = options.match;
        matchFields.forEach(function (prop) {
          body.query.bool.must.push(getMatchObj(prop));
        });
      }

      if (options.range) {
        body.query.bool.must.push(getRangeObj(options.range));
      }

      if (options.sort) {
        body.sort = options.sort;
      }

      return body;
    }
  }
};

function getMatchObj(matcher) {
  var obj = {
    match: {}
  };
  obj.match = matcher;
  return obj;
}

function getRangeObj(ranger) {
  var obj = {
    range: {}
  };
  obj.range = ranger;
  return obj;
}

module.exports = es_ops;

// -- Test Code ---------------------------------------------------------
if (require.main === module) {
  (function () {

    var userOpt = process.argv[2];

    var options = {
      size: '10',
      match: [{
        field1: 'value1'
      }, {
        field2: 'value2'
      }],
      range: {
        field1: {
          'gte': '2015-12-11T13:26:45.211Z',
          'lte': '2015-12-16T13:26:45.211Z'
        }
      },
      sort: [{
        'updatedAt': {
          'order': 'desc'
        }
      }],
      aggs: [{
        aggName: 'aggName',
        field: 'fieldName',
        size: 15
      }]
    };

    if (userOpt)
      options = userOpt;

    console.log(JSON.stringify(es_ops.getQuery(options)));
  })();
}
