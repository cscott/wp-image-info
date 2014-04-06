require('prfun'); // es6 and lots of promises

var request = Promise.guard(25, Promise.promisify(require('request')));
var qs = require('querystring');
var sqlite3 = require('sqlite3').verbose();

var userAgent = 'ImageInfo/0.1';
var parsoidApi = "http://parsoid-lb.eqiad.wikimedia.org/enwiki/";

var DBFILE = "enwiki-images.db";
var PAGEDB = "enwiki-pages.db";

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

var createResultDb = function(filename) {
    var db = new Db(filename);
    return db.run("CREATE TABLE IF NOT EXISTS "+
                  "page (name TEXT, pageid INTEGER PRIMARY KEY, "+
                  "parsoid TEXT, error BOOLEAN DEFAULT false);").return(db);
};

var fetchPage= function(title, pageid) {
    var args = {};
    if (pageid) { args.oldid = pageid; }
    var uri = parsoidApi + encodeURIComponent(title) + '?' + qs.stringify(args);
    var requestOptions = {
        method: 'GET',
        followRedirect: true,
        uri: uri,
        timeout: 5 * 60 * 1000,
        headers: {
            'User-Agent': userAgent
        }
    };
    return request(requestOptions).caught(function(e) {
        return (e instanceof Error && e.code === 'ETIMEDOUT');
    }, function() {
        // wait a minute and then retry if we got ETIMEDOUT
        console.log('ETIMED OUT',uri);
        return Promise.delay(60 * 1000).then(function() {
            return request(requestOptions);
        });
    }).spread(function(response, body) {
        return { statusCode: response.statusCode, body: body };
    });
};

var counter = 0;
var processPage = function(pagedb, title, pageid) {
    return fetchPage(title, pageid).then(function(resp) {
        if (resp.statusCode !== 200) {
            return pagedb.run(
                "INSERT OR REPLACE INTO page (name, pageid, error) "+
                    "VALUES (?,?,?);",
                title, pageid, true);
        }
        return pagedb.run(
            "INSERT OR REPLACE INTO page (name, pageid, parsoid) "+
                "VALUES (?,?,?);",
            title, pageid, resp.body);
    }).then(function() {
        console.log((++counter)+' '+title+' ['+pageid+']');
    });//.catch(function(e) { console.log('boo', e); throw e; });
};

exports.main = function() {
    return createResultDb(PAGEDB).then(function(pagedb) {
        // fetch the set of unique pages which include images
        var p = Promise.resolve();
        var each = function(err, row) {
            if (err) {
                p = p.throw(err);
            } else {
                p = p.return(processPage(pagedb, row.pagename, row.pageid));
            }
        };
        var db = new Db(DBFILE);
        return db.each("SELECT DISTINCT pagename,pageid FROM usage ASC;", each).
            then(function() { return p; }).finally(function() {
                return Promise.join(db.close(), pagedb.close());
            });
    }).done();
};
