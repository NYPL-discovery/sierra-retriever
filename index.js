console.log("Loading sierra resource retriever service");

const avro = require('avsc');
const AWS = require('aws-sdk')
const crypto = require('crypto')
const kinesis = new AWS.Kinesis({region: 'us-east-1'})
const wrapper = require('sierra-wrapper')
const config = require('config')
const retry = require('retry')

//main function
exports.handler = function(event, context, callback) {
  var record = event.Records[0];
  if(record.kinesis){
    kinesisHandler(event.Records, context, callback);
  }
};

//kinesis stream handler
var kinesisHandler = function(records, context, callback) {
  console.log('Processing ' + records.length + ' records');

  var conf = {
    key: config.get('key'),
    secret: config.get('secret'),
    base: config.get('base')
  }
  wrapper.loadConfig(conf)

  // Make sure wrapper_access_token is set:
  set_wrapper_access_token()
    // Make sure schema is fetched:
    .then(getSchema)
    // Now we're oauthed and have a parsed schema, so process the records:
    .then((schema) => processResources(records, schema))
    // Now tell the lambda enviroment whether there was an error or not:
    .then((results) => {
      var successes = 0
      var failures = 0
      results.forEach((result, ind) => {
        if (result.error) failures += 1
        else successes += 1
      })
      var error = null
      if (failures > 0) error = `${failures} failed`
      callback(error, `Wrote ${records.length}; Succeeded: ${successes} Failures: ${failures}`)
    })
    .catch((error) => {
      console.log('calling callback with error')
      callback(error)
    })
}

var __schemaObject = null;

var getSchema = function () {
  // If we've already fetched it, return immediately:
  if (__schemaObject) return Promise.resolve(__schemaObject)

  // Otherwise, fetch it for first time:
  var schema_retrieval_url = null;
  if(config.get('isABib')){
   schema_retrieval_url = config.get('bib.schema_retrieval_url');
  }else{
    schema_retrieval_url = config.get('item.schema_retrieval_url');
  }
  return schema(schema_retrieval_url)
    .then((schemaObj) => {
      __schemaObject = schemaObj
      return __schemaObject
    })
}

var setGlobalAccessTokenAndProcessResources = (function(records, schema_data){
  // If we already have it, return immediately:
  if (wrapper_access_token) return Promise.resolve()

  return set_wrapper_access_token()
        .then(function(access_token){
          console.log('Access token is - ' + access_token);
          wrapper_access_token = access_token;
          processResources(records, schema_data);
        });
  });

//process resources
var processResources = function(records, schema_data){
  // records.forEach(function(record){
  return Promise.all(
    records.map((record) => {
      var data = record.kinesis.data;
      var json_data = avro_decoded_data(schema_data, data);
      if(config.get('isABib')){
        return resource_in_detail(json_data.id, true)
          .then(function(bib) {
            var stream = config.get('bib.stream');
            return postResourcesToStream(bib, stream)
              .then(() => ({ error: null, bib: bib }))
          })
          .catch((e) => {
            console.log('Error with bib: ', e)
            return { error: e, bib: json_data.id }
          })
      }else{
        return resource_in_detail(json_data.id, false)
          .then(function(item){
            var stream = config.get('item.stream');
            return postResourcesToStream(item, stream)
              .then(() => ({ error: null, item }))
          })
          .catch((e) => {
            console.log('Error with item: ', e)
            return { error: e, item: json_data.id }
          })
      }
    })
  );
}

//get schema
var request = require('request');
var schema = function(url) {
  return new Promise(function(resolve, reject) {
      console.log('Querying for schema - ' + url);
      request(url, function(error, response, body) {
        if(!error && response.statusCode == 200){
          resolve(JSON.parse(body).data.schema);
        } else {
          console.log("An error occurred - " + response.statusCode);
          reject("Error occurred while getting schema - " + response.statusCode);
        }
      })
    })
}

//use avro to deserialize
var avro_decoded_data = function(schema_data, record){
  const type = avro.parse(schema_data);
  var decoded = new Buffer(record, 'base64');
  var verify = type.fromBuffer(decoded);
  return JSON.parse(verify);
}

//get full bib/item details for each bib/item id
var resource_in_detail = function(id, isBib){
  return new Promise(function(resolve, reject){
    var operation = retry.operation({
        retries: 5,
        factor: 3,
        minTimeout: 1 * 1000,
        maxTimeout: 60 * 1000,
        randomize: true,
    });
    operation.attempt(function(currentAttempt) {
      if(isBib){
        console.log('Requesting for bib info');
        wrapper.requestSingleBib(id, (errorBibReq, results) => {
          getResult(errorBibReq, results, true, id, operation, currentAttempt)
              .then(function(entry){
                resolve (entry);
              })
              .catch (function(e) {
                reject (e);
              });
          });
      } else{
        console.log('Requesting for item info');
        var itemIds = [id];
        wrapper.requestMultiItemBasic(itemIds, (errorItemReq, results) => {
          getResult(errorItemReq, results, false, itemIds, operation, currentAttempt)
            .then(function(entry){
              resolve (entry);
            })
            .catch (function(e) {
              reject (e);
            });
        });
      }
    });
  });
}

//get bib or item based on prama passed
var getResult = function(errorResourceReq, results, isBib, resourceId, operation, attemptNumber){
  return new Promise(function(resolve, reject){
    if(errorResourceReq){
        if(errorResourceReq.httpStatus !== null && errorResourceReq.httpStatus === 401){
          console.log("This is a token issue. Going to renew token");
          if(isBib) {
            console.log('Number of attempts made for bib ' + resourceId + ' - ' + attemptNumber);
          } else{ 
            console.log('Number of attempts made for item ' + resourceId + ' - ' + attemptNumber);
          }
          set_wrapper_access_token()
            .then(function (access_token){
              wrapper_access_token = access_token;
              if(operation.retry(errorResourceReq)){
                return;
              }
              if(isBib){
                  console.log('Error occurred while getting bib info');
              } else {
                  console.log('Error occurred while getting item info');
              }
                reject(errorResourceReq);
            });
        }
      } else {
            var entries = results.data.entries;
            var entry = entries[0];
            console.log(JSON.stringify(entry));
            resolve(entry);
          }
  });
}

var __access_token = null

//get wrapper access token
var set_wrapper_access_token = function(){
  if (__access_token) return Promise.resolve(__access_token)

  return new Promise(function(resolve, reject){
    wrapper.auth(function (error, authResults){
      if(error){
        console.log(error, error.stack);
        reject ("Error occurred while getting access token");
      }
      __access_token = wrapper.authorizedToken
      resolve (wrapper.authorizedToken);
    });
  });
}

//send data to kinesis Stream
var postResourcesToStream = function(resource, stream){
  return new Promise((resolve, reject) => {
    const type = avro.infer(resource);
    const resource_in_avro_format = type.toBuffer(resource);
    var params = {
      Data: resource_in_avro_format, // required 
      PartitionKey: crypto.randomBytes(20).toString('hex').toString(), // required
      StreamName: stream, // required 
    }
    kinesis.putRecord(params, function (err, data) {
      // if (err) console.log(err, err.stack) // an error occurred
      // else     console.log(data)           // successful response
      console.log('wrote to kinesis: ', data)
      if (err) reject(err)
      else resolve(data)
      //cb(null,data)
    })
  })
}
  
