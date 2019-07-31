/**
 * Populate newEvent.json with bib/item ids of your choice
 *
 * Usage: node kinesify-data [id(s)] [Sierra(Bib|Item)RetrievalRequest]
 *
 * Examples:
 *   // This builds an event for a single item:
 *   node kinesify-data 36007311 SierraItemRetrievalRequest
 *
 *   // This builds an event for multiple items:
 *   node kinesify-data 36007311,36007312,36007313 SierraItemRetrievalRequest
*/

const fs = require('fs')
const NyplClient = require('@nypl/nypl-data-api-client')
var client = new NyplClient({ base_url: 'https://platform.nypl.org/api/v0.1/' })
var avroType
const recordNumbers = process.argv[2].split(',')
const schemaName = process.argv[3]
kinesify = function (record, avroType) {
  // encode avro
  var buf
  buf = avroType.toBuffer(record)
  // encode base64
  var encoded = buf.toString('base64')
  // kinesis format
  return {
    'kinesis': {
      'kinesisSchemaVersion': '1.0',
      'partitionKey': 's1',
      'sequenceNumber': '00000000000000000000000000000000000000000000000000000001',
      'data': encoded,
      'approximateArrivalTimestamp': 1428537600
    },
    'eventSource': 'aws:kinesis',
    'eventVersion': '1.0',
    'eventID': 'shardId-000000000000:00000000000000000000000000000000000000000000000000000001',
    'eventName': 'aws:kinesis:record',
    'invokeIdentityArn': 'arn:aws:iam::EXAMPLE',
    'awsRegion': 'us-east-1',
    // We depend on the ARN ending in /Bib or /Item to determine how to decode the payload
    // Everything up to that is ignored
    'eventSourceARN': `the-first-part-of-the-arn-does-not-matter...this-part-does:/SierraBibRetrievalRequest`
  }
}
client.get(`current-schemas/${schemaName}`, { authenticate: false }).then((resp) => {
  let schema = resp
  // Now we can build an avro encoder by parsing the escaped "schema" prop:
  avroType = require('avsc').parse(JSON.parse(schema.schema))
}).then(() => {
  const records = recordNumbers
    .map((recordNumber) => ({ id: recordNumber }))
    .map((recordNumberDocument) => kinesify(recordNumberDocument, avroType))

  let json = JSON.stringify({ Records: records }, null, 2)
  fs.writeFile('./newEvent.json', json) // `{ \"Records\":\n [\n${json}\n] }`, () => {} )
})
