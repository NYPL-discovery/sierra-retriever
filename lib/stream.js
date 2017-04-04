const avro = require('avsc')
const AWS = require('aws-sdk')
const crypto = require('crypto')
const kinesis = new AWS.Kinesis({region: 'us-east-1'})

// send data to kinesis Stream
exports.postResourcesToStream = function (resource, stream, schema_object) {
  return new Promise((resolve, reject) => {
    const type = avro.parse(schema_object)
    const resource_in_avro_format = type.toBuffer(resource)
    var params = {
      Data: resource_in_avro_format, // required
      PartitionKey: crypto.randomBytes(20).toString('hex').toString(), // required
      StreamName: stream // required
    }
    kinesis.putRecord(params, function (err, data) {
      console.log('wrote to kinesis: ', data)
      if (err) reject(err)
      else resolve(data)
    })
  })
}
