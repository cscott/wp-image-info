// Promise-returning wrappers for sqlite

require('prfun');
var sqlite3 = require('sqlite3');

var Db = module.exports = function(filename, options) {
    options = options || {};
    var mode = options.readonly ? sqlite3.OPEN_READONLY :
        ( sqlite3.OPEN_CREATE | sqlite3.OPEN_READWRITE );
    var db = new sqlite3.Database(filename, mode);
    this.get = Promise.promisify(db.get, db);
    // eventually yields the number of returned rows
    // XXX suboptimal because the node module wa
    this.each = Promise.method(function() {
        var args = Array.from(arguments);
        var cb = args.pop();
        var d = Promise.defer();
        args.push(function(err, row) {
            // XXX note that the node module fetches the next row as soon
            // as this function returns, it doesn't wait for the promise
            // to be resolved.  This means that the database access tends
            // to run way ahead of the processing, causing excessive
            // memory consumption in the task queue.
            if (err) return d.reject(err);
            try {
                Promise.resolve(cb(row)).catch(d.reject).done();
            } catch (e) { d.reject(e); }
        });
        args.push(d.callback);
        console.log('ARGUMENTS', args);
        db.each.apply(db, args);
        return d.promise;
    });
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
