const avro = require('avsc')
const crypto = require('crypto')
const bunyan = require('bunyan')
const config = require('config')
const NyplStreamsClient = require('@nypl/nypl-streams-client')

var nyplBaseUrl = null
if (!(config.get('nyplBaseUrl').endsWith('/'))) {
  nyplBaseUrl = config.get('nyplBaseUrl').concat('/')
}
console.log(nyplBaseUrl)
var streamsClient = new NyplStreamsClient({ nyplDataApiClientBase: nyplBaseUrl})

const isABib = process.env.RETRIEVAL_TYPE === 'bib'
var log = isABib ? bunyan.createLogger({name: 'sierra-bib-retriever'}) : bunyan.createLogger({name: 'sierra-item-retriever'})

// send data to kinesis Stream
exports.streamPoster = (records, streamName) => {
  return writeToStreamsClient(streamName, records)
}

var writeToStreamsClient = (streamName, records) => {
  return streamsClient.write(streamName, records)
          .then((resp) => {
            if(resp.FailedRecordCount > 0){
              var failedRecords = getFailedRecords(resp, records)
              return writeToStreamsClient(streamName, failedRecords)
            }
            log.info('Finished writing to stream. Records - ' + resp.Records.length)
            return Promise.resolve({sent: records.length, receieved: resp.Records.length})
          })
          .catch((error) => {
            log.error('Error occurred while posting to kinesis')
            return Promise.reject(error)
          })
}

var getFailedRecords = (response, recordsThatWerePosted) => {
  var records = response.Records
  var failedRecords = []
  for (var i = 0; i < records.length; i++) {
    if (records[i].ErrorCode) {
      failedRecords.push(recordsThatWerePosted[i])
    }
  }
  log.info('Failed records count : ' + failedRecords.length)
  return failedRecords
}
