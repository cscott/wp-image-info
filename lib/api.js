require('prfun'); // es6 and lots of promises

var request = exports.request = Promise.guard(25, require('./request'));
var Config = require('./config');

exports.apiRequest = Promise.method(function(apiArgs) {
    apiArgs.format = 'json';
    var requestOptions = {
        method: 'GET',
        followRedirect: true,
        uri: Config.wpApi,
        qs: apiArgs,
        timeout: 5 * 60 * 1000,
        headers: {
            'User-Agent': Config.userAgent
        }
    };
    return request(requestOptions).caught(function(e) {
        return (e instanceof Error /*&& e.code === 'ETIMEDOUT'*/);
    }, function(e) {
        // wait a minute and then retry if we got ETIMEDOUT
        console.log('ETIMED OUT', e.code, requestOptions.uri);
        return Promise.delay(60 * 1000).then(function() {
            return request(requestOptions);
        });
    }).spread(function(response, body) {
        console.assert(response.statusCode === 200);
        return JSON.parse(body);
    });
});
