/**
 * Module dependencies.
 */

var express = require('express');
var routes = require('./routes');
var user = require('./routes/user');
var http = require('http');
var path = require('path');
var fs = require('fs');
//var querystring = require('querystring');

var app = express();

var mkdirp = require('./mkdirp');

var port = 50070
var hdfs = "./dfs"

// all environments
app.set('port', process.env.PORT || port);
app.set('views', __dirname + '/views');
app.set('view engine', 'jade');
app.use(express.favicon());
app.use(express.logger('dev'));
app.use(function (req, res, next) {
    var data = '';
    req.setEncoding('utf8');
    req.on('data', function (chunk) {
        data += chunk;
    });

    req.on('end', function () {
        req._body = data;
        next();
    });
});
app.use(express.bodyParser());
app.use(express.methodOverride());
app.use(app.router);
app.use(express.static(path.join(__dirname, 'public')));

// development only
if ('development' == app.get('env')) {
    app.use(express.errorHandler());
}


app.get('/', routes.index);
//app.get('/users', user.list);


var appendPath = '/webhdfs/v1append'

app.post('/webhdfs/v1/*', function (req, res) {

    if (req.query.op === 'APPEND') {
        var path = req.path.replace(/^\/webhdfs\/v1/, '')

        //console.log(req.originalUrl)
        //console.log(req.host)

        var to = req.protocol + '://' + req.host + ':' + port +
            req.originalUrl.replace(/^\/webhdfs\/v1/, appendPath)
        console.log('redirect:', to)
        res.redirect(307, to)
    } else {
        res.send(200)
    }
})

app.post(appendPath + '/*', function (req, res) {

    var path = req.path.replace(/^\/\w+\/[^/]+/, '')
    //var data = clean(req._body)
    var data = req._body
    //console.log(req.originalUrl)
    //console.log("======================")
    //console.log(data)
    //console.log("======================")

    output(path, data, function () {
        res.send(200)
    })
    //var to =
    //res.redirect(301, '/webhdfs/v1.1');
})

/*
 var clean = function (str) {
 var len = str.length
 return str.substring(0, str.length - 1)
 }
 */

var output = function (p, data, cb) {
    var f = hdfs + p
    console.log("Write " + data.length + " data to " + f)

    fs.appendFile(f, data, function (err) {
        if (err) console.error(err)

        // no directory
        if (err && err.code === "ENOENT") {
            //console.log("dir:" + path.dirname(f))
            mkdirp(path.dirname(f), function (err) {

                if (err)
                    console.error(err)

                fs.appendFile(f, data, function (err) {
                    if (err) console.error(err);
                    (typeof cb === "function") && cb()
                })
            })
        } else {
            (typeof cb === "function") && cb()
        }
    })
}

http.createServer(app).listen(app.get('port'), function () {
    console.log('Express server listening on port ' + app.get('port'));
});


