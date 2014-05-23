require('prfun'); // es6 and lots of promises
// regenerate thumbnails
var PARALLEL = 20;

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

var fetchImageInfo = function(resource) {
    var apiArgs = {
        action: 'query',
        prop: 'imageinfo',
        titles: resource,
        iiprop: 'mediatype|size'
    };
    return apiRequest(apiArgs).then(function(result) {
        try {
            var pages = result.query.pages;
            var idx = Object.keys(pages)[0];
            return pages[idx].imageinfo[0];
        } catch (e) {
            console.warn("COULDN'T FIND IMAGE SIZE", result);
            return null;
        }
    });
};
var fetchImageThumb = function(resource, size) {
    var apiArgs = {
        action: 'query',
        prop: 'imageinfo',
        titles: resource,
        iiprop: 'url',
        iiurlwidth: size,
        iiurlheight: size
    };
    return apiRequest(apiArgs).then(function(result) {
        try {
            var pages = result.query.pages;
            var idx = Object.keys(pages)[0];
            return pages[idx].imageinfo[0];
        } catch (e) {
            console.warn("COULDN'T FIND IMAGE URL", result);
            return null;
        }
    }).then(function(info) {
        if (!info) return;
        var url = info.thumburl;
        //console.log('FOUND URL!', url);
        return request({
            //method: 'HEAD',
            uri: url,
            headers: {
                'User-Agent': Config.userAgent
            },
            jar: true,
            lengthOnly: true
        }).spread(function(resp, contents) {
            console.log('SCALED', resource, contents);
        });
    });
};

var examineFigure = function(tasks, figure) {
    // XXX keep a persistent list of what figures we've fetched already?
    var figtype = figure.getAttribute('typeof');
    if (figtype !== 'mw:Image/Thumb')
        return; // only look at thumbs
    if (!figure.classList.contains('mw-default-size'))
        return; // only look at default-size thumbs
    var resource = figure.querySelector('img[resource]');
    if (!resource) return; // wha?
    resource = resource.getAttribute('resource').replace(/^([.][.]?\/)*/, '');
    var dp = JSON.parse(figure.getAttribute('data-parsoid')||'{}');
    // I guess this is an interesting figure
    tasks.push(fetchImageInfo(resource).then(function(imageinfo) {
        if (!imageinfo) return; // no image info, boo
        if (imageinfo.height <= imageinfo.width) return; // landscape
        //console.log('GROOVY', resource, imageinfo);
        // fix to 220x220px bounding box
        return fetchImageThumb(resource, 220);
    }));
}

var fetchPage= function(title, pageid) {
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
            retries < 5) {
            retries++;
            pageid = response.headers.location.replace(/^.*[?]oldid=/, '');
            return fetchPage(title, pageid);
        }
        if (response.statusCode===503) {
            retries++;
            console.log('ERROR ('+retries+')', response.statusCode, uri);
            // wait half a minute and then retry if we got ETIMEDOUT
            requestOptions.timeout *= 2;
            if (retries < 5) return Promise.delay(30 * 1000).then(doOne);
            // give up
            return [{statusCode:666}, ''];
        }
        return { statusCode: response.statusCode, body: body };
    });
};

var counter = 0;
var processPage = function(title, pageid) {
    var numfigs = -1, pageid = 0;
    return fetchPage(title, pageid).then(function(resp) {
        if (resp.statusCode !== 200) {
            console.warn('SKIPPING', resp.statusCode, title, pageid);
            return; // boo.
        }
        var tasks = [];
        var document = domino.createDocument(resp.body);
        // pull out pageid
        var about = document.querySelector('html[about]');
        if (about) {
            about = +about.getAttribute('about').replace(/^.*\//g, '');
            if (about) pageid = about;
        }
        // pull out figures
        var figures = document.querySelectorAll('*[typeof^="mw:Image"]');
        for (var i=0; i<figures.length; i++) {
            examineFigure(tasks, figures[i]);
        }
        numfigs = figures.length;
        return Promise.all(tasks);
    }).then(function() {
        console.log((++counter)+' '+Config.prefix+':'+title+' ['+pageid+'] ('+numfigs+')');
    });
};

var queuePage = (function() {
    var outstanding = 0;
    return function(title, pageid, next) {
        outstanding++;
        var p = processPage(title, pageid).
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

var CHUNK = 100; // 100 pages at a time
var getPagesFromMWApi = function(apcontinue) {
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

    var retries = 0;
    return apiRequest(apiArgs).then(function(json) {
        console.assert(json && json.query, apcontinue, json);
        var qcontinue =
            json['query-continue'] &&
            json['query-continue'].allpages &&
            json['query-continue'].allpages.apcontinue;
        var pages = json.query.allpages;
        var i = 0;
        var next = function() {
            if (i < pages.length) {
                i++;
                return queuePage(pages[i-1].title, pages[i-1].pageid, next);
            }
        };
        return next().then(function() {
            if (qcontinue)
                return getPagesFromMWApi(qcontinue);
        });
    }, function(e) {
        // hm, pause & retry this whole section
        retries++;
        console.log('ERROR ('+retries+')', e, apcontinue);
        // wait half a minute and then retry
        if (retries < 5)
            return Promise.delay(30 * 1000).then(function() {
                return getPagesFromMWApi(apcontinue);
            });
    });
};

exports.main = function() {
    return getPagesFromMWApi(AFTER).done();
};
