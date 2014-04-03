require('prfun'); // es6 and lots of promises

var request = Promise.promisify(require('request'));
var qs = require('querystring');
var sqlite3 = require('sqlite3').verbose();

var userAgent = 'ImageInfo/0.1';
var apiURI = 'https://en.wikipedia.org/w/api.php';

var OUTFILE = "enwiki-images.db";
var CHUNK = 50; // fetch info about 50 images at a time.

var apiRequest = Promise.method(function(apiArgs) {
    apiArgs.format = 'json';
    var uri = apiURI + '?' + qs.stringify( apiArgs );
    var requestOptions = {
	method: 'GET',
	followRedirect: true,
	uri: uri,
	timeout: 40 * 1000,
	headers: {
	    'User-Agent': userAgent,
	}
    };
    return request(requestOptions).spread(function(response, body) {
	console.assert(response.statusCode === 200);
	return JSON.parse(body);
    });
});

var imageInfo = Promise.method(function(titles) {
    if (!Array.isArray(titles)) { titles = [titles]; }
    var props = [
	'mediatype',
	'size'
    ];
    var apiArgs = {
	action: 'query',
	format: 'json',
	prop: 'imageinfo',
	titles: titles.join( '|' ),
	iiprop: props.join( '|' )
    };
    return apiRequest(apiArgs).then(function(result) {
	var pages = result.query.pages;
	return Object.keys(pages).reduce(function(o, pageid) {
	    var title = pages[pageid].title;
	    o[title] = pages[pageid].imageinfo[0];
	    return o;
	}, Object.create(null));
    });
});

var imageUsage = Promise.method(function(title, qcontinue) {
    var apiArgs = {
	action: 'query',
	format: 'json',
	list: 'imageusage',
	iutitle: title,
	iunamespace: 0,
	iulimit: CHUNK
    };
    if (qcontinue) { apiArgs.iucontinue = qcontinue; }
    return apiRequest(apiArgs).then(function(json) {
	var qcontinue =
	    json['query-continue'] &&
	    json['query-continue'].imageusage &&
	    json['query-continue'].imageusage.iucontinue;
	var chunk = json.query.imageusage;
	var result = { chunk: chunk };
	if (qcontinue) {
	    //result.next = imageUsage(title, qcontinue);
	}
	return result;
    });
});


var fetchImages = function(qcontinue) {
    var props = [
	'mediatype',
	'size'
    ];
    var apiArgs = {
	action: 'query',
	prop: 'imageinfo',
	iiprop: props.join( '|' ),
	format: 'json',
	iilimit: CHUNK,
	generator: 'allimages',
	gailimit: CHUNK
    };
    if (qcontinue) { apiArgs.gaicontinue = qcontinue; }
    return apiRequest(apiArgs).then(function(json) {
	console.assert(json && json.query, qcontinue, json);
	var qcontinue =
	    json['query-continue'] &&
	    json['query-continue'].allimages &&
	    json['query-continue'].allimages.gaicontinue;
	var pages = json.query.pages;
	var chunk = Object.keys(pages).reduce(function(a, pageid) {
	    a.push(pages[pageid]);
	    return a;
	}, []).map(function(pi) {
	    pi.imageinfo = pi.imageinfo[0];
	    return pi;
	});
	var result = { chunk: chunk };
	if (qcontinue) {
	    result.next = fetchImages(qcontinue);
	}
	return result;
    });
};

var createDb = function(filename) {
    return new Promise(function(resolve, reject) {
	var db = new sqlite3.Database(filename, function(err) {
	    if (err) { return reject(err); }
	    resolve(db);
	});
    });
};

exports.main = function() {
    var db = createDb(OUTFILE);
    // fetch all images in enwiki
    var processImageChunk = function(images) {
	var tasks = images.chunk.map(function(i) {
	    // record i.pageid, i.title,
	    // i.imageinfo.width, i.imageinfo.height,
	    // i.imageinfo.mediatype
	    //console.log(i.title);
	    // look up pages which use this image
	    var processUsageChunk = function(usage) {
		usage.chunk.forEach(function(u) {
		    // record u.pageid, u.title
		    console.log(i.title, 'used on', u.title);
		});
		if (usage.next) {
		    return usage.next.then(processUsageChunk);
		}
	    };
	    return imageUsage(i.title).then(processUsageChunk);
	});
	var p = Promises.all(tasks);
	if (images.next) {
	    p = Promise.join(p, images.next.then(processImageChunk));
	}
	return p;
    };
    db.then(function() {
	return fetchImages().then(processImageChunk);
    }).then(function() {
	return Promise.promisify(db.close, db)();
    }).done();
};

// usage examples
if (false) {

    imageInfo(['File:!Hero (album).jpg','File:"52" (no. 21, front cover).jpg']).then(function(result) {
	console.log(JSON.stringify(result));
    }).done();

    fetchImages().then(function(result) {
	console.log(JSON.stringify(result));
    }).done();

    imageUsage('File:Albert Einstein Head.jpg').then(function(result) {
	console.log(JSON.stringify(result));
    }).done();

}
