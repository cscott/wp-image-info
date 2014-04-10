require('prfun'); // es6 and lots of promises
var domino = require('domino');

var Config = require('./config');
var Db = require('./db');
var ImageInfo = require('./imageinfo');

var countTotalFigures = function(pagedb) {
    return pagedb.get("SELECT count(*) AS total FROM figure;").get('total');
};

var countFramedFigures = function(pagedb) {
    return pagedb.get('SELECT count(*) AS total FROM figure WHERE '+
                      'typeof = "mw:Image/Frame";').get('total');
};

var countAllImages = function(imagedb) {
    return imagedb.get("SELECT count(*) AS total FROM image "+
                       "WHERE mediatype IN (?,?);",
                       "BITMAP", "DRAWING").get('total');
};

var countPortraitImages = function(imagedb) {
    return imagedb.get("SELECT count(*) AS total FROM image "+
                       "WHERE height > width AND mediatype IN (?,?);",
                       "BITMAP", "DRAWING").get('total');
};

var forEachRow = function(db, query, f) {
    return db.prepare(query).then(function(stmt) {
        var next = function() {
            //console.log('next');
            return stmt.get().then(handleRow);
        };
        var handleRow = function(row) {
            //console.log('handle row', row);
            if (!row) return; // done!
            return Promise.resolve(f(row)).then(next);
        };
        return next().finally(function() {
            return stmt.finalize().catch(function(e) {
                console.log('error during finalize:', e);
            });
        });
    });
};

var forEachFigure = function(db, query, f) {
    return forEachRow(db, query, function(row) {
        var document = domino.createDocument(row.figure);
        var figure = document.body.firstElementChild;
        var dp = JSON.parse(figure.getAttribute('data-parsoid')||'{}');
        var resource = figure.querySelector('img').
            getAttribute('resource').replace(/^([.][.]?\/)*/, '');
        return f({ figure: figure, dp: dp, resource: resource, row: row });
    });
};


var getImageInfo = function(imagedb, title) {
    // first try to get the image info from the database we build
    return imagedb.get("SELECT width, height, mediatype FROM image "+
                       "WHERE name = ?;", title).
        then(function(row) {
            if (!row) {
                // try to use the API to look this up
                return ImageInfo.imageInfo(title).then(function(obj) {
                    return obj[Object.keys(obj)[0]];
                });
            }
            return row;
        });
};

var checkAffected = exports.check = function(size, info) {
    var m = /(\d*)(?:x(\d*))?(?:px)?/.exec(size);
    var width = m[1], height = m[2];
    return true;
};

// for https://gerrit.wikimedia.org/r/116995
var countChangedFramed = function(pagedb, imagedb) {
    var affected = [];
    return forEachFigure(
        pagedb,
        'SELECT name, figid, figure FROM figure WHERE typeof="mw:Image/Frame";',
        function(f) {
            var figure = f.figure, dp = f.dp, resource = f.resource, row=f.row;
            var width;
            (dp.optList || []).forEach(function(opt) {
                if (opt.ck === 'width') { width = opt.ak; }
            });
            if (width && /^\s*\d*x\d+(px)?\s*$/.test(width)) {
                // look at image dimensions, see if it's really
                // affected
                return getImageInfo(imagedb, resource).then(function(ii) {
                    if (checkAffected(width, ii)) {
                        console.log(row.name, resource, width,
                                    ii.width, ii.height);
                        affected.push({page:row.name, image:resource,
                                       wt: width, ii: ii});
                    }
                });
            }
        }).then(function() { return affected.length; });
};

// for https://gerrit.wikimedia.org/r/119332
var countChangedBogus = function(pagedb) {
    var affected = [], cnt = 0;
    return forEachFigure(
        pagedb,
        'SELECT name, figid, figure FROM figure;',
        function(f) {
            var figure = f.figure, dp = f.dp, resource = f.resource, row=f.row;
            var bogosity = (dp.optList || []).filter(function(opt) {
                return opt.ck === 'bogus' && opt.ak.trim() !== '';
            });
            if (bogosity.length === 0) return; // boo-ring
            console.log(bogosity.map(function(opt){return opt.ak.trim();}));
        }).then(function() { return affected.length; });
};

exports.main = function() {
    var pagedb = new Db(Config.pageDb);
    var imagedb = new Db(Config.imageDb);
    return Promise.props({
        allImages: countAllImages(imagedb), // non-commons images
        portraitImages: countPortraitImages(imagedb),

        totalFigures: countTotalFigures(pagedb), // image inclusions on pages
        framedFigures: countFramedFigures(pagedb),
        changedFramed: countChangedFramed(pagedb, imagedb),
        changedBogus: countChangedBogus(pagedb)
    }).then(function(results) {
        console.log(results);
    }).finally(function() {
        return Promise.join(pagedb.close(), imagedb.close()).catch(function(e){
            console.log("Couldn't close db:", e);
        });
    }).done();
};
