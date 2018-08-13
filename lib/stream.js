const bunyan = require('bunyan')
const config = require('config')
const NyplStreamsClient = require('@nypl/nypl-streams-client')

var nyplBaseUrl = null
if (!(config.get('nyplBaseUrl').endsWith('/'))) {
  nyplBaseUrl = config.get('nyplBaseUrl').concat('/')
}
var streamsClient = new NyplStreamsClient({nyplDataApiClientBase: nyplBaseUrl})

const isABib = process.env.RETRIEVAL_TYPE === 'bib'
var log = isABib ? bunyan.createLogger({name: 'sierra-bib-retriever'}) : bunyan.createLogger({name: 'sierra-item-retriever'})

// send data to kinesis Stream
exports.streamPoster = (records, streamName, schemaName) => {
  return writeToStreamsClient(streamName, records, schemaName)
          .then(() => {
            return Promise.resolve(records)
          })
          .catch((error) => {
            return Promise.reject(error)
          })
}

// this function should resolve until FailedRecordCount is zero
var writeToStreamsClient = (streamName, records, schemaName) => {
  return streamsClient.write(streamName, records, { avroSchemaName: schemaName })
          .then((resp) => {
            if (resp.FailedRecordCount > 0) {
              var failedRecords = getFailedRecords(resp, records)
              return writeToStreamsClient(streamName, failedRecords, schemaName)
            }
            log.info('Finished writing to stream. Records - ' + resp.Records.length)
            return Promise.resolve(resp)
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
