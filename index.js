console.log("Loading sierra resource retriever service");

const avro = require('avsc');
const AWS = require('aws-sdk')
const crypto = require('crypto')
const kinesis = new AWS.Kinesis({region: 'us-east-1'})
const wrapper = require('sierra-wrapper')
const config = require('config')
const retry = require('retry')

var schema_stream_retriever = null;
var wrapper_access_token = null;

//main function
exports.handler = function(event, context){
  var record = event.Records[0];
  if(record.kinesis){
    kinesisHandler(event.Records, context);
  }
};

//kinesis stream handler
var kinesisHandler = function(records, context) {
  console.log('Processing ' + records.length + ' records');
  wrapper.loadConfig('./config/default.json');
  if(schema_stream_retriever === null){
    var schema_retrieval_url = null;
    if(config.get('isABib')){
     schema_retrieval_url = config.get('bib.schema_retrieval_url');
    }else{
      schema_retrieval_url = config.get('item.schema_retrieval_url');
    }
    schema(schema_retrieval_url)
      .then(function(schema_data){
        schema_stream_retriever = schema_data;
        if(wrapper_access_token === null){
            setGlobalAccessTokenAndProcessResources(records, schema_data);
        }else{
          processResources(records, schema_data);
        }
      })
    .catch(function(e){
      console.log(e, e.stack);
    });
  }else{
    if(wrapper_access_token === null){
      setGlobalAccessTokenAndProcessResources(records, schema_stream_retriever);
    }else{
      processResources(records, schema_stream_retriever);
    }
  }
}

var setGlobalAccessTokenAndProcessResources = (function(records, schema_data){
  set_wrapper_access_token()
        .then(function(access_token){
          console.log('Access token is - ' + access_token);
          wrapper_access_token = access_token;
          processResources(records, schema_data);
        });
  });

//process resources
var processResources = function(records, schema_data){
  records.forEach(function(record){
      var data = record.kinesis.data;
      var json_data = avro_decoded_data(schema_data, data);
      if(config.get('isABib')){
        resource_in_detail(json_data.id, true)
          .then(function(bib) {
            var stream = config.get('bib.stream');
            postResourcesToStream(bib, stream);
          })
          .catch(function (e){
            console.log(e);
          });
      }else{
        resource_in_detail(json_data.id, false)
          .then(function(item){
            var stream = config.get('item.stream');
            postResourcesToStream(item, stream);
          })
      }
    });
}

//get schema
var request = require('request');
var schema = function(url) {
  return new Promise(function(resolve, reject) {
      console.log('Querying for schema - ' + url);
      request(url, function(error, response, body) {
        if(!error && response.statusCode == 200){
          resolve(JSON.parse(body).data.schema);
        }else{
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
                if(isBib){
                  console.log('Error occurred while getting bib info');
                } else {
                  console.log('Error occurred while getting item info');
                }
                reject(errorResourceReq);
              }
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

//get wrapper access token
var set_wrapper_access_token = function(){
  return new Promise(function(resolve, reject){
    wrapper.auth(function (error, authResults){
      if(error){
        console.log(error, error.stack);
        reject ("Error occurred while getting access token");
      }
      resolve (wrapper.authorizedToken);
    });
  });
}

//send data to kinesis Stream
var postResourcesToStream = function(resource, stream){
  const type = avro.infer(resource);
  const resource_in_avro_format = type.toBuffer(resource);
  var params = {
    Data: resource_in_avro_format, // required 
    PartitionKey: crypto.randomBytes(20).toString('hex').toString(), // required
    StreamName: stream, // required 
  }
  kinesis.putRecord(params, function (err, data) {
    if (err) console.log(err, err.stack) // an error occurred
    else     console.log(data)           // successful response
    //cb(null,data)
  })
}
  