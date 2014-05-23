require('prfun'); // es6 and lots of promises
var PARALLEL = 8;

var qs = require('querystring');
var domino = require('domino');

var Api = require('./api');
var Config = require('./config');
var Db = require('./db');

var apiRequest = Api.apiRequest;
var request = Api.request;

var AFTER=process.env.WPAFTER || '';
var ONLY_ERRORS=false;      // only fetch pages that failed the first time
var PREFETCHED_PAGES=false; // don't fetch any pages, just parse images
var SAVE_ONLY_IMAGES=true;  // don't store full page source

// fetch all pages, not just the ones we think have images.
// (this catches pages which have images only from commons)
var FETCH_ALL_PAGES=true;

process.setMaxListeners(0); // since we've 20 concurrent redirects.

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
    var uri = Config.parsoidApi + Config.prefix + '/' + encodeURIComponent(title);
    var requestOptions = {
        method: 'GET',
        followRedirect: false,
        uri: uri,
        qs: args,
        timeout: 30 * 1000,
        headers: {
            'User-Agent': Config.userAgent
        },
        jar: true
    };
    var isTimedOut = function(e) {
        return (e instanceof Error /*&& e.code === 'ETIMEDOUT'*/);
    };
    var retries = 0;
    var doOne = function() {
        return request(requestOptions).caught(isTimedOut, function(e) {
            retries++;
            console.log('ERROR ('+retries+')', e.code, uri);
            // wait half a minute and then retry if we got ETIMEDOUT
            requestOptions.timeout *= 2;
            if (retries < 5) return Promise.delay(30 * 1000).then(doOne);
            // give up
            return [{statusCode:666}, ''];
        });
    };

    return doOne().spread(function(response, body) {
        if ((response.statusCode==301 || response.statusCode==302) &&
            !pageid) {
            pageid = response.headers.location.replace(/^.*[?]oldid=/, '');
            return fetchPage(pagedb, title, pageid);
        }
        return { statusCode: response.statusCode, body: body };
    });
};

var counter = 0;
var maybeProcessPage = function(pagedb, title) {
    if (ONLY_ERRORS) {
        return pagedb.get("SELECT error FROM page WHERE name = ?;", title).
            then(function(row) {
                if (row && row.error)
                    return processPage(pagedb, title);
            });
    }
    return processPage(pagedb, title);
};

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
        } else if (ONLY_ERRORS && SAVE_ONLY_IMAGES) {
            tasks.push(pagedb.run(
                "DELETE FROM page WHERE name = ? AND error = 1;", title
            ));
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
        console.log((++counter)+' '+Config.prefix+':'+title+' ['+pageid+'] ('+numfigs+')');
    });
};

var queuePage = (function() {
    var outstanding = 0;
    return function(pagedb, title, next) {
        outstanding++;
        var p = maybeProcessPage(pagedb, title).
            finally(function() { outstanding--; });
        if (outstanding < PARALLEL) {
            // increate parallelism
            return Promise.join(p, next());
        } else {
            // too much outstanding, execute serially
            return p.then(next);
        }
    };
})();

var getPagesFromImageDb = function(pagedb) {
    // fetch the set of unique pages which include images
    var db = new Db(Config.imageDb);
    return db.prepare("SELECT DISTINCT pagename FROM usage WHERE pagename > ? ORDER BY pagename ASC;", AFTER).
        then(function(stmt) {
            var outstanding = 0;
            var handleRow = function(row) {
                if (!row) return; // done
                return queuePage(pagedb, row.pagename, function() {
                    return stmt.get().then(handleRow);
                });
            };
            return stmt.get().then(handleRow).finally(function() {
                return stmt.finalize();
            });
        }).finally(function() {
            return db.close();
        });
};

var CHUNK = 100; // 100 pages at a time
var getPagesFromMWApi = function(pagedb, apcontinue) {
    var apiArgs = {
        action: 'query',
        list: 'allpages',
        apnamespace: 0,
        apfilterredir: 'nonredirects',
        aplimit: CHUNK
    };
    if (apcontinue) {
        apiArgs.apcontinue = apcontinue;
        console.log('ap', apcontinue);
    }


    return apiRequest(apiArgs).then(function(json) {
        console.assert(json && json.query, apcontinue, json);
        var qcontinue =
            json['query-continue'] &&
            json['query-continue'].allpages &&
            json['query-continue'].allpages.apcontinue;
        var pages = json.query.allpages;
        var i = 0;
        var next = function() {
            if (i < pages.length)
                return queuePage(pagedb, pages[i++].title, next);
        };
        return next().then(function() {
            if (qcontinue)
                return getPagesFromMWApi(pagedb, qcontinue);
        });
    });
};

exports.main = function() {
    return createResultDb(Config.pageDb).then(function(pagedb) {
        var p = FETCH_ALL_PAGES ?
            getPagesFromMWApi(pagedb, AFTER) : getPagesFromImageDb(pagedb);
        return p.finally(function() {
            return pagedb.close();
        });
    }).done();
};
