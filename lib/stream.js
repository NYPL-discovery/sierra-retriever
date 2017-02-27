const AWS = require('aws-sdk')
const _ = require('highland')
const crypto = require('crypto')

const kinesis = new AWS.Kinesis({region: 'us-east-1'})

exports.postBibsStream = function(bibs,callback){
  var params = {
    Data: JSON.stringify(bibs), /* required */
    PartitionKey: crypto.randomBytes(20).toString('hex').toString(), /* required */
    StreamName: 'SierraBibPostRequest', /* required */
  }
  kinesis.putRecord(params, function (err, data) {
    if (err) console.log(err, err.stack) // an error occurred
    else     console.log(data)           // successful response
    cb(null,data)
  })

}
