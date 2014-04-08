require('prfun'); // es6 and lots of promises
var PARALLEL = 10;

var request = Promise.guard(2*PARALLEL, Promise.promisify(require('request')));
var qs = require('querystring');
var sqlite3 = require('sqlite3');//.verbose();
var domino = require('domino');

var userAgent = 'ImageInfo/0.1';
var parsoidApi = "http://parsoid-lb.eqiad.wikimedia.org/";
var PREFIX = process.env.WPPREFIX || 'enwiki';

var AFTER=process.env.WPAFTER || '';
var PREFETCHED_PAGES=false;
var SAVE_ONLY_IMAGES=true;

var DBFILE = PREFIX + "-images.db";
var PAGEDB = PREFIX + "-pages.db";

process.setMaxListeners(0); // since we've 20 concurrent redirects.

var Db = function(filename, options) {
    options = options || {};
    var mode = options.readonly ? sqlite3.OPEN_READONLY :
        ( sqlite3.OPEN_CREATE | sqlite3.OPEN_READWRITE );
    var db = new sqlite3.Database(filename, mode);
    this.get = Promise.promisify(db.get, db);
    this.each = Promise.promisify(db.each, db);
    this.run = Promise.promisify(db.run, db);
    this.close = Promise.promisify(db.close, db);
    this.prepare = function() {
        var args = Array.from(arguments);
        var d = Promise.defer();
        args.push(d.callback);
        var statement = db.prepare.apply(db, args);
        return d.promise.return({
            get: Promise.promisify(statement.get, statement),
            finalize: Promise.promisify(statement.finalize, statement)
        });
    };
};

var createResultDb = function(filename) {
    var db = new Db(filename);
    return db.run("CREATE TABLE IF NOT EXISTS "+
                  "figure (name TEXT, pageid INTEGER, "+
                  "figid INTEGER, typeof TEXT, figure TEXT, "+
                  "PRIMARY KEY(name, figid));").
        then(function() {
            return db.run("CREATE TABLE IF NOT EXISTS "+
                          "page (name TEXT PRIMARY KEY, pageid INTEGER, "+
                          "parsoid TEXT, error BOOLEAN DEFAULT false);");
        }).return(db);
};

var fetchPageFromDb = function(pagedb, title) {
    return pagedb.get("SELECT parsoid FROM page WHERE name = ?;", title).
	then(function(row) {
	    if (!row) { return { statusCode: 404, body: '' }; }
	    return { statusCode: 200, body: row.parsoid };
	});
};

var fetchPage= function(pagedb, title, pageid) {
    if (PREFETCHED_PAGES)
	return fetchPageFromDb(pagedb, title);
    var args = {};
    if (pageid) { args.oldid = pageid; }
    var uri = parsoidApi + PREFIX + '/' + encodeURIComponent(title);
    var requestOptions = {
        method: 'GET',
        followRedirect: true,
        uri: uri,
	qs:args,
        timeout: 1 * 60 * 1000,
	followRedirect: false,
        headers: {
            'User-Agent': userAgent
        }
    };
    var isTimedOut = function(e) {
	return (e instanceof Error /*&& e.code === 'ETIMEDOUT'*/);
    };
    var retries = 0;
    var doOne = function() {
	return request(requestOptions).caught(isTimedOut, function(e) {
	    retries++;
	    console.log('ERROR ('+retries+')', e.code, uri);
            // wait a minute and then retry if we got ETIMEDOUT
	    if (retries < 5) return Promise.delay(60 * 1000).then(doOne);
	    // give up
	    return [{statusCode:666}, ''];
	});
    };

    return doOne().spread(function(response, body) {
        return { statusCode: response.statusCode, body: body };
    });
};

var counter = 0;
var processPage = function(pagedb, title) {
    var numfigs = -1, pageid = 0;
    return fetchPage(pagedb, title).then(function(resp) {
        if (resp.statusCode !== 200) {
	    if (PREFETCHED_PAGES) return;
            return pagedb.run(
                "INSERT OR REPLACE INTO page (name, error) "+
                    "VALUES (?,?);",
                title, true);
        }
        var tasks = [];
        var document = domino.createDocument(resp.body);
	// pull out pageid
	var about = document.querySelector('html[about]');
	if (about) {
	    about = +about.getAttribute('about').replace(/^.*\//g, '');
	    if (about) pageid = about;
	}
        if (!(PREFETCHED_PAGES || SAVE_ONLY_IMAGES)) {
            tasks.push(pagedb.run(
                "INSERT OR REPLACE INTO page (name, pageid, parsoid, error) "+
                    "VALUES (?,?,?,?);",
                title, pageid, resp.body, false));
        }
        // pull out figures
        var figures = document.querySelectorAll('*[typeof^="mw:Image"]');
        for (var i=0; i<figures.length; i++) {
            tasks.push(pagedb.run("INSERT OR REPLACE INTO figure "+
                                  "(name, pageid, figid, typeof, figure) "+
                                  "VALUES (?,?,?,?,?)", title, pageid, i,
                                  figures[i].getAttribute('typeof'),
                                  figures[i].outerHTML));
        }
	numfigs = figures.length;
        return Promise.all(tasks);
    }).then(function() {
        console.log((++counter)+' '+PREFIX+':'+title+' ['+pageid+'] ('+numfigs+')');
    });
};

exports.main = function() {
    return createResultDb(PAGEDB).then(function(pagedb) {
        // fetch the set of unique pages which include images
        var db = new Db(DBFILE);
        return db.prepare("SELECT DISTINCT pagename FROM usage WHERE pagename > ? ORDER BY pagename ASC;", AFTER).
            then(function(stmt) {
                var outstanding = 0;
                var handleRow = function(row) {
                    //console.log('handle row', outstanding);
                    if (!row) return; // done
                    outstanding++;
                    var p = processPage(pagedb, row.pagename).
                        finally(function() { outstanding--; });
                    var next = function() {
                        return stmt.get().then(handleRow);
                    };
                    if (outstanding < PARALLEL) {
                        // increase parallelism
                        return Promise.join(p, next());
                    } else {
                        // too much outstanding, execute serially
                        return p.then(next);
                    }
                };
                return stmt.get().then(handleRow).finally(function() {
                    return stmt.finalize();
                });
            }).finally(function() {
                return Promise.join(db.close(), pagedb.close());
            });
    }).done();
};
