require('prfun'); // es6 and lots of promises

var request = Promise.guard(25, Promise.promisify(require('request')));
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
	    'User-Agent': userAgent
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


var fetchImages = function(gaicontinue) {
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
    if (gaicontinue) { apiArgs.gaicontinue = gaicontinue; }
    return apiRequest(apiArgs).then(function(json) {
	console.assert(json && json.query, gaicontinue, json);
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

var Db = function(filename, options) {
    options = options || {};
    var mode = options.readonly ? sqlite3.OPEN_READONLY :
        ( sqlite3.OPEN_CREATE | sqlite3.OPEN_READWRITE );
    var db = new sqlite3.Database(filename, mode);
    this.get = Promise.promisify(db.get, db);
    this.each = Promise.promisify(db.each, db);
    this.run = Promise.promisify(db.run, db);
    this.close = Promise.promisify(db.close, db);
};

var createDb = function(filename) {
    var db = new Db(filename);
// name is unique
    return db.run("CREATE TABLE IF NOT EXISTS "+
                  "image (name TEXT PRIMARY KEY, pageid INTEGER, "+
                  "width INT, height INT, bound INT, mediatype TEXT);").
        then(function() {
            return db.run("CREATE TABLE IF NOT EXISTS "+
                          "usage (imagename TEXT, pagename TEXT, "+
                          "pageid INTEGER, PRIMARY KEY(imagename,pagename));");
        }).return(db);
};

var max = function(a, b) { return (a>b) ? a : b; };

exports.main = function() {
    var counter = 0;
    createDb(OUTFILE).then(function(db) {
        // fetch all images in enwiki
        var processImageChunk = function(images) {
	    var tasks = images.chunk.map(function(i) {
	        // record i.pageid, i.title,
	        // i.imageinfo.width, i.imageinfo.height,
	        // i.imageinfo.mediatype
                var p = db.run(
                    "INSERT OR REPLACE INTO image "+
                        "(name, pageid, width, height, bound, mediatype) "+
                        "VALUES (?,?, ?, ?, ?, ?);",
                    i.title, +i.pageid, +i.imageinfo.width, +i.imageinfo.height,
                    max(+i.imageinfo.width, +i.imageinfo.height),
                    i.imageinfo.mediatype);
	        // look up pages which use this image
	        var processUsageChunk = function(usage) {
		    var tasks = usage.chunk.map(function(u) {
		        // record u.pageid, u.title
                        return db.run(
                            "INSERT OR REPLACE INTO usage "+
                                "(imagename, pagename, pageid) VALUES (?,?,?);",
                            i.title, u.title, u.pageid);
		    });
		    if (usage.next) {
                        tasks.push(usage.next.then(processUsageChunk));
		    }
                    return Promise.all(tasks);
	        };
                if (/^(BITMAP|DRAWING)$/.test(i.imageinfo.mediatype)) {
                    // skip usage for video/sounds/etc
                    p = imageUsage(i.title).then(processUsageChunk).return(p);
                }
	        return p.then(function() {
                    console.log((++counter)+' '+i.title, i.imageinfo.mediatype);
                });
	    });
	    if (images.next) {
                tasks.push(images.next.then(processImageChunk));
	    }
	    return Promise.all(tasks);
        };
        return fetchImages().then(processImageChunk);
    }).then(function() {
        return db.close();
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
