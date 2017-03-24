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
    schema(config.get('schema_retrieval_url'))
    .then(function(schema_data){
      schema_stream_retriever = schema_data;
      if(wrapper_access_token === null){
        set_wrapper_access_token()
        .then(function(access_token){
          console.log('Access token is - ' + access_token);
          wrapper_access_token = access_token;
          processResources(records, schema_data);
        })
      }else{
        processResources(records, schema_data);
      }
    })
    .catch(function(e){
      console.log(e, e.stack);
    });
  }else{
    if(wrapper_access_token === null){
      set_wrapper_access_token()
      .then(function(access_token){
        console.log('Access token is - ' + access_token);
        wrapper_access_token = access_token;
        processResources(records, schema_stream_retriever);
      })
    }else{
      processResources(records, schema_stream_retriever);
    }
  }
}

//process resources
var processResources = function(records, schema_data){
  records.forEach(function(record){
      var data = record.kinesis.data;
      var json_data = avro_decoded_data(schema_data, data);
      bib_in_detail(json_data.id)
          .then(function(bib) {
            postBibsStream(bib);
          })
          .catch(function (e){
            console.log(e);
          });
    });
}

//get schema
var request = require('request');
var schema = function(url, context) {
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

//get full bib details for each bib
var bib_in_detail = function(bibId){
  return new Promise(function(resolve, reject){
    var operation = retry.operation({
        retries: 5,
        factor: 3,
        minTimeout: 1 * 1000,
        maxTimeout: 60 * 1000,
        randomize: true,
    });
    operation.attempt(function(currentAttempt) {
      wrapper.requestSingleBib(bibId, (errorBibReq, results) => {
      if(errorBibReq){
        if(errorBibReq.httpStatus !== null && errorBibReq.httpStatus === 401){
          console.log("This is a token issue. Going to renew token");
          console.log('Number of re-attempts for bib ' + bibId + ' - ' + currentAttempt);
          set_wrapper_access_token()
          .then(function (access_token){
            wrapper_access_token = access_token;
            if(operation.retry(errorBibReq))
              return;
          });
        }
          console.log('Error occurred while getting bib info');
          reject(errorBibReq);
        } else {
            var entries = results.data.entries;
            var entry = entries[0];
            console.log(JSON.stringify(entry));
            resolve(entry);
          }
      });
    });
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
var postBibsStream = function(bib){
  const type = avro.infer(bib);
  const bib_in_avro_format = type.toBuffer(bib);
  var params = {
    Data: bib_in_avro_format, // required 
    PartitionKey: crypto.randomBytes(20).toString('hex').toString(), // required
    StreamName: config.get('stream'), // required 
  }
  kinesis.putRecord(params, function (err, data) {
    if (err) console.log(err, err.stack) // an error occurred
    else     console.log(data)           // successful response
    //cb(null,data)
  })
}
  