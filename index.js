console.log("Loading sierra resource retriever service");

const avro = require('avsc');
const AWS = require('aws-sdk')
const crypto = require('crypto')
const kinesis = new AWS.Kinesis({region: 'us-east-1'})
const wrapper = require('sierra-wrapper')
const config = require('config')

var schema_stream_retriever = null;

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
  if(schema_stream_retriever === null){
    schema(config.get('schema_retrieval_url'))
    .then(function(schema_data){
      schema_stream_retriever = schema_data;
      processResources(records, schema_data);
    })
    .catch(function(e){
      console.log(e, e.stack);
    });
  }else{
    processResources(records, schema_stream_retriever);
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
      });
    })
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
var loadedConfig = wrapper.loadConfig('./config/default.json');
var bib_in_detail = function(bibId) {
  return new Promise(function (resolve, reject) {
    wrapper.auth((error, authResults) => {
      if (error){
        console.log(error);
        reject("Error occurred while getting bib details - " + error);
      }
      wrapper.requestSingleBib(bibId, (errorBibReq, results) => {
        if(errorBibReq) console.log(errorBibReq);
        var entries = results.data.entries;
        var entry = entries[0];
        console.log(JSON.stringify(entry));
        resolve(entry);
      });
    });
  })
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
  