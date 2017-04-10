var auth_key = process.env.AUTH_KEY || '';
var mqtt_host = process.env.MQTT_HOST || '';
var mqtt_user = process.env.MQTT_USER || '';
var mqtt_pass = process.env.MQTT_PASS || '';
var http_port = process.env.PORT || 5000;
var debug_mode = process.env.DEBUG_MODE || false;

var mqtt = require('mqtt');
var express = require('express');
var bodyParser = require('body-parser');
var async = require('async');
var _ = require('underscore');

var app = express();

app.set('port', http_port);
app.use(bodyParser.json());


app.post('/post/', function(req, res) {

  Promise.resolve(req)
  .then(r => {
    logRequest(r);
    validateRequest(r);
    return { key: r.body['key'], message: r.body['message'], topics: _.isString(r.body['topic']) ? [ r.body['topic'] ] : r.body['topic'] };
  })
  .then(r => {
    authorise(r.key);
    console.log('Authorisation success');
    return r;
  })
  .then(r => {
    return publish(r.topics, r.message);
  })
  .then(x => {
    console.log('Publish successful');
    res.send('ok');
  })
  .catch(err => {
    console.log(err);
    res.send('error');
  });
  

});

app.listen(app.get('port'), function() {
  console.log('Node app is running on port', app.get('port'));
});

function logRequest(req) {
  var ip = req.headers['x-forwarded-for'] ||
           req.connection.remoteAddress;
  var message = 'Received request [' + req.originalUrl + 
              '] from [' + ip + ']';
  if (debug_mode) {
    message += ' with payload [' + JSON.stringify(req.body) + ']';
  } else {
    message += '.';
  }
  console.log(message);
}

function validateRequest(req) {
  if( _.isUndefined(req.body['topic']) || req.body['topic'].length == 0 )
    throw new Error('Bad request, topic is not defined');
  if( _.isUndefined(req.body['message']) || req.body['message'].length == 0 || !_.isString(req.body['message']) )
    throw new Error('Bad request, message is not defined');
  if( _.isUndefined(req.body['key']) || req.body['key'].length == 0 || !_.isString(req.body['key']) )
    throw new Error('Bad request, key is not defined');
}

function authorise(key) {
  if( key === null || key.length < 10 ) 
    throw new Error('Invalid authorisation supplied');
  if( !auth_key || auth_key === null || auth_key.length < 10 )
    throw new Error('Authorisation configuration error');
  if( auth_key === key )
    return true;
  throw new Error('Authorisation failed');
}

function publish(topics, msg) {
    return new Promise( (res,rej) => {
        var client = mqtt.connect(mqtt_host, {username: mqtt_user, password: mqtt_pass});
        client.on('connect', () => {
           console.log('MQTT Connected'); 
        });
        client.on('error', err => {
           rej(err);
           client.end();
           console.log('MQTT connection closed'); 
        });
        async.each( topics, (topic,callback)=>{
            console.log('Publishing '+msg+' to '+topic);
            client.publish(topic,msg,err=> {
                callback(err);
            });
        }, err =>{
           err ? rej(err) : res();
           client.end();
           console.log('MQTT connection closed'); 
        });
    });         
}
