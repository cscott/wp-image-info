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

var countChangedFramed = function(pagedb, imagedb) {
    var affected = [];
    return pagedb.prepare('SELECT name, figid, figure FROM figure WHERE '+
                          'typeof = "mw:Image/Frame";').
        then(function(stmt) {
            var next = function() {
                return stmt.get().then(handleRow);
            };
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
                        getAttribute('resource').replace(/^([.][.]?\/)*/, '');
                    // look at image dimensions, see if it's really
                    // affected
                    return getImageInfo(imagedb, resource).then(function(ii) {
                        if ((+ii.height) > (+ii.width)) {
                            console.log(row.name, resource, width,
                                        ii.width, ii.height);
                            affected.push({page:row.name, image:resource,
                                           wt: width, ii: ii});
                        } else {
                            console.log("framed landscape", row.name, resource);
                        }
                    }).return(next()/*get started right away*/);
                }
                return next();
            };
            return next().finally(function() {
                return stmt.finalize();
            }).then(function() { return affected.length; });
        });
};

exports.main = function() {
    var pagedb = new Db(Config.pageDb);
    var imagedb = new Db(Config.imageDb);
    return Promise.props({
        allImages: countAllImages(imagedb), // non-commons images
        portraitImages: countPortraitImages(imagedb),

        totalFigures: countTotalFigures(pagedb), // image inclusions on pages
        framedFigures: countFramedFigures(pagedb),
        changedFramed: countChangedFramed(pagedb, imagedb)

    }).then(function(results) {
        console.log(results);
    }).finally(function() {
        return Promise.join(pagedb.close(), imagedb.close()).catch(function(e){
            console.log("Couldn't close db:", e);
        });
    }).done();
};
