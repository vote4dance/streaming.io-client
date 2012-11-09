var fs = require('fs');

exports.version = require('../package.json').version;

exports.build = function(callback) {
    fs.readFile(__dirname + '/streaming.io.js', callback);
}