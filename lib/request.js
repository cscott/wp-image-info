// Promisify the request package.
// Actually hyperquest, to disable some request annoyances

require('prfun'); // es6 and lots of promises
var hyperquest = require('hyperquest');

var qs = require('querystring');
var util = require('util');
var stream = require('stream');

var StringStream = function() {
    stream.Writable.call(this, { decodeStrings: false });
    this.str = '';
};
util.inherits(StringStream, stream.Writable);
StringStream.prototype._write = function(chunk, encoding, callback) {
    if (typeof(chunk) !== 'string') {
        chunk = chunk.toString('utf-8');
    }
    this.str += chunk;
    callback();
};
StringStream.prototype.toString = function() {
    return this.str;
};

var request = module.exports = function(requestOptions) {
    return new Promise(function(resolve, reject) {
        // return Promise of [response, body]
        if (requestOptions.qs) {
            if (typeof(requestOptions.qs) !== 'string') {
                requestOptions.qs = qs.stringify(requestOptions.qs);
            }
            requestOptions = Object.create(requestOptions);
            requestOptions.uri += '?' + requestOptions.qs;
        }
        var r = hyperquest(requestOptions);
        var ss = new StringStream();
        var resp = null;
        r.pipe(ss, { end: false });
        r.on('response', function(response) {
            resp = response;
        });
        r.on('end', function() {
            resolve([resp, ss.toString()]);
        });
        r.on('error', function(e) {
            reject(e);
        });
    }).spread(function(resp, s) {
        if (requestOptions.format === 'json') {
            return [resp, JSON.parse(s)];
        }
        return [resp, s];
    });
};
