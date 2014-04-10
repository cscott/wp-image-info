// hacky hacky shared filenames & etc

var Config = module.exports = {
    userAgent: 'ImageInfo/0.2 (cananian@wikimedia.org)',
    prefix: process.env.WPPREFIX || 'enwiki',
    parsoidApi: "http://parsoid-lb.eqiad.wikimedia.org/"
};

if (!/wiki$/.test(Config.prefix)) { Config.prefix += 'wiki'; }

Config.wpApi = "https://" + Config.prefix.replace(/wiki$/,'') +
    ".wikipedia.org/w/api.php";
Config.imageDb = Config.prefix + '-images.db';
Config.pageDb = Config.prefix + '-pages.db';
