const AWS = require('aws-sdk')
const crypto = require('crypto')
const kinesis = new AWS.Kinesis({region: 'us-east-1'})




// var params = {
//   StreamName: 'SierraBibPostRequestTest' /* required */
// };
// kinesis.describeStream(params, function(err, data) {
//   if (err) console.log(err, err.stack); // an error occurred
//   else     console.log(JSON.stringify(data,null,2));           // successful response

  var id = crypto.randomBytes(20).toString('hex').toString()

  var params = {
    Data: JSON.stringify(['whats up','whats up','whats up','whats up','whats up']), /* required */
    PartitionKey: id, /* required */
    StreamName: 'SierraBibPostRequestTest'
  }
  kinesis.putRecord(params, function (err, data) {
    if (err) console.log(err, err.stack) // an error occurred
    else     console.log(data)           // successful response

  })


// });




