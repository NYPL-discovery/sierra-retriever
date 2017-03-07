console.log("Loading sierra bib retriever");

const avro = require('avsc');
const AWS = require('aws-sdk')
const crypto = require('crypto')
const kinesis = new AWS.Kinesis({region: 'us-east-1'})

console.log("Going to call main function");

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
    .then(function(data) {
      console.log(data);
      context.done();
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

// var params = {
//   StreamName: 'SierraBibPostRequestTest' /* required */
// };
// kinesis.describeStream(params, function(err, data) {
//   if (err) console.log(err, err.stack); // an error occurred
//   else     console.log(JSON.stringify(data,null,2));           // successful response

  // var id = crypto.randomBytes(20).toString('hex').toString()
  //
  // var params = {
  //   Data: JSON.stringify(['whats up','whats up','whats up','whats up','whats up']), /* required */
  //   PartitionKey: id, /* required */
  //   StreamName: 'SierraBibPostRequestTest'
  // }
  // kinesis.putRecord(params, (err, data) => {
  //   if (err) console.log(err, err.stack) // an error occurred
  //   else     console.log(data)           // successful response
  //
  // })


// });
