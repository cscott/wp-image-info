// Promise-returning wrappers for sqlite

require('prfun');
var sqlite3 = require('sqlite3');

var Db = module.exports = function(filename, options) {
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
