//config.js
'use strict';

var CONFIG={
	FROM:{
		ES_URL: 'localhost:9200',
		ES_INDEX: 'catalog',
		ES_TYPE: 'refiner',
		TOUT: 200000
	},

	TO:{
		ES_URL: 'localhost:9200',
		ES_INDEX: 'latest',
		ES_TYPE: 'refiner',
		TOUT: 200000
	}
};

module.exports = CONFIG;