require('prfun'); // es6 and lots of promises
var domino = require('domino');

var Config = require('./config');
var Db = require('./db');

var countTotalImages = function(pagedb) {
    return pagedb.get("SELECT count(*) AS total FROM figure;").
        then(function(row) { return row.total; });
};

var countFramedImages = function(pagedb) {
    return pagedb.get('SELECT count(*) AS total FROM figure WHERE '+
                      'typeof = "mw:Image/Frame";').
        then(function(row) { return row.total; });
};

var countChangedFramed = function(pagedb) {
    var affected = 0;
    return pagedb.prepare('SELECT name, figid, figure FROM figure WHERE '+
                          'typeof = "mw:Image/Frame";').
        then(function(stmt) {
            var handleRow = function(row) {
                if (!row) return; // done!
                //console.log(row.name, row.figid, row.figure);
                var document = domino.createDocument(row.figure);
                var figure = document.body.firstElementChild;
                var dp = JSON.parse(figure.getAttribute('data-parsoid')||'{}');
                var width;
                (dp.optList || []).forEach(function(opt) {
                    if (opt.ck === 'width') { width = opt.ak; }
                });
                if (width && /^\s*\d*x\d+(px)?\s*$/.test(width)) {
                    var resource = figure.querySelector('img').
                        getAttribute('resource');
                    console.log(row.name, resource, width);
                    // XXX look at image dimensions, see if it's really
                    // affected
                    affected++;
                }
                return stmt.get().then(handleRow); // next
            };
            return stmt.get().then(handleRow).finally(function() {
                return stmt.finalize();
            }).then(function() { return affected; });
        });
};

exports.main = function() {
    var pagedb = new Db(Config.pageDb);
    return Promise.props({
        totalImages: countTotalImages(pagedb),
        framedImages: countFramedImages(pagedb),
        changedImages: countChangedFramed(pagedb)
    }).then(function(results) {
        console.log(results);
    }).finally(function() { return pagedb.close(); }).done();
};
