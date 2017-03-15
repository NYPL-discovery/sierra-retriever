console.log("Loading sierra bib retriever");

const avro = require('avsc');
const AWS = require('aws-sdk')
const crypto = require('crypto')
const kinesis = new AWS.Kinesis({region: 'us-east-1'})
const wrapper = require('sierra-wrapper')

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
  var url = "https://api.nypltech.org/api/v0.1/current-schemas/SierraItemRetrievalRequest";
  schema(url)
    .then(function(schema_data) {
      records.forEach(function(record){
        var data = record.kinesis.data;
        var json_data = avro_decoded_data(schema_data, data);
        bib_in_detail(json_data.id)
          .then(function(bib) {
            postBibsStream(bib);
          })
      });
    })
    .catch(function(e){
      console.log(e);
    });
}

//get schema
var request = require('request');
var schema = function(url, context) {
  return new Promise(function(resolve, reject) {
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
var loadedConfig = wrapper.loadConfig('config.json');
var bib_in_detail = function(bibId) {
  return new Promise(function (resolve, reject) {
    wrapper.auth((error, results) => {
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
  console.log(type.getSchema());
  const bib_in_avro_format = type.toBuffer(bib);
  var params = {
    Data: bib_in_avro_format, /* required */
    PartitionKey: crypto.randomBytes(20).toString('hex').toString(), /* required */
    StreamName: 'testBibRetrieval', /* required */
  }
  kinesis.putRecord(params, function (err, data) {
    if (err) console.log(err, err.stack) // an error occurred
    else     console.log(data)           // successful response
    //cb(null,data)
  })

}
