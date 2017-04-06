console.log('Loading sierra resource retriever service')

const avro = require('avsc')
const wrapper = require('sierra-wrapper')
const config = require('config')
const retry = require('retry')
const schemaHelper = require('./lib/schema-helper')
var streams = require('./lib/stream')

const SCHEMAREADINGSTREAM = 'schemaReadingStream'
const SCHEMAPOSTINGSTREAM = 'schemaPostingStream'

// main function
exports.handler = function (event, context, callback) {
  var record = event.Records[0]
  if (record.kinesis) {
    kinesisHandler(event.Records, context, callback)
  }
}

// kinesis stream handler
var kinesisHandler = function (records, context, callback) {
  console.log('Processing ' + records.length + ' records')

  var conf = {
    key: config.get('key'),
    secret: config.get('secret'),
    base: config.get('base')
  }
  wrapper.loadConfig(conf)

  // Make sure wrapper_accessToken is set:
  getWrapperAccessToken()
    // Make sure all the schemas are fetched:
    .then(() => {
      return getSchemas()
    })
    // Now we're oauthed and have parsed schemas, so process the records:
    .then((schemas) => {
      return processResources(records, schemas)
    })
    // Now tell the lambda enviroment whether there was an error or not:
    .then((results) => {
      var successes = 0
      var failures = 0
      results.forEach((result, ind) => {
        if (result.error) failures += 1
        else successes += 1
      })
      var error = null
      if (failures > 0) error = `${failures} failed`
      callback(error, `Wrote ${records.length}; Succeeded: ${successes} Failures: ${failures}`)
    })
    .catch((error) => {
      console.log('calling callback with error')
      callback(error)
    })
}

// General purpose global hash of things to remember:
var CACHE = {}

var getSchemas = () => {
  // If we've already fetched it, return immediately:
  if (Object.keys(CACHE).length !== 0) {
    return Promise.resolve(CACHE)
  }
  // Otherwise, fetch it for first time:
  var schemaReadingStream = null
  var schemaPostingStream = null
  if (config.get('isABib')) {
    schemaReadingStream = config.get('bib.schemaReadingStream')
    schemaPostingStream = config.get('bib.schemaPostingStream')
  } else {
    schemaReadingStream = config.get('item.schemaReadingStream')
    schemaPostingStream = config.get('item.schemaPostingStream')
  }
  return Promise.all([
    schemaHelper.schema(schemaReadingStream),

    schemaHelper.schema(schemaPostingStream)

  ])
      .then((allSchemas) => {
        console.log('Sending all schemas')
        CACHE[SCHEMAREADINGSTREAM] = allSchemas[0]
        CACHE[SCHEMAPOSTINGSTREAM] = allSchemas[1]
        return Promise.resolve(CACHE)
      })
      .catch((e) => {
        return Promise.reject(e)
      }
    )
}

// process resources
var processResources = function (records, schemas) {
  // records.forEach(function(record){
  return Promise.all(
    records.map((record) => {
      var data = record.kinesis.data
      var jsonData = avroDecodedData(schemas[SCHEMAREADINGSTREAM], data)
      if (config.get('isABib')) {
        return resourceInDetail(jsonData.id, true)
          .then(function (bib) {
            var stream = config.get('bib.stream')
            return streams.postResourcesToStream(bib, stream, schemas[SCHEMAPOSTINGSTREAM])
              .then(() => ({ error: null, bib: bib }))
              .catch((e) => {
                console.log('Error occurred posting to kinesis - ' + e)
                return ({ error: e, bib: bib })
              })
          })
          .catch((e) => {
            console.log('Error with bib: ' + jsonData.id, e)
            return { error: e, bib: jsonData.id }
          })
      } else {
        return resourceInDetail(jsonData.id, false)
          .then(function (item) {
            var stream = config.get('item.stream')
            return streams.postResourcesToStream(item, stream, schemas[SCHEMAPOSTINGSTREAM])
              .then(() => ({ error: null, item }))
          })
          .catch((e) => {
            console.log('Error with item: ', e)
            return { error: e, item: jsonData.id }
          })
      }
    })
  )
}

// use avro to deserialize
var avroDecodedData = function (schemaData, record) {
  const type = avro.parse(schemaData)
  var decoded = new Buffer(record, 'base64')
  var verify = type.fromBuffer(decoded)
  return JSON.parse(verify)
}

// get full bib/item details for each bib/item id
var resourceInDetail = function (id, isBib) {
  return new Promise(function (resolve, reject) {
    var operation = retry.operation({
      retries: 5,
      factor: 3,
      minTimeout: 1 * 1000,
      maxTimeout: 60 * 1000,
      randomize: true
    })
    operation.attempt(function (currentAttempt) {
      if (isBib) {
        console.log('Requesting for bib info')
        wrapper.requestSingleBib(id, (errorBibReq, results) => {
          getResult(errorBibReq, results, true, id, operation, currentAttempt)
              .then(function (entry) {
                console.log(JSON.stringify(entry))
                resolve(entry)
              })
              .catch(function (e) {
                console.log('Error occurred while getting bib - ' + e)
                reject(e)
              })
        })
      } else {
        console.log('Requesting for item info')
        var itemIds = [id]
        wrapper.requestMultiItemBasic(itemIds, (errorItemReq, results) => {
          getResult(errorItemReq, results, false, itemIds, operation, currentAttempt)
            .then(function (entry) {
              console.log(JSON.stringify(entry))
              resolve(entry)
            })
            .catch(function (e) {
              console.log('Error occurred while getting item - ' + e)
              reject(e)
            })
        })
      }
    })
  })
}

// get bib or item based on switch isBib passed
var getResult = function (errorResourceReq, results, isBib, resourceId, operation, attemptNumber) {
  return new Promise(function (resolve, reject) {
    if (errorResourceReq) {
      let errorInJson = JSON.parse(errorResourceReq)
      console.log('Error httpstatus -' + errorInJson.httpStatus + '-')
      if (errorInJson.httpStatus === 401) {
        console.log('This is a token issue. Going to renew token')
        if (isBib) {
          console.log('Number of attempts made for bib ' + resourceId + ' - ' + attemptNumber)
        } else {
          console.log('Number of attempts made for item ' + resourceId + ' - ' + attemptNumber)
        }
        getWrapperAccessToken()
            .then(function (accessToken) {
              if (operation.retry(errorResourceReq)) {
                return
              }
              if (isBib) {
                console.log('Error occurred while getting bib info')
              } else {
                console.log('Error occurred while getting item info')
              }
              reject(errorResourceReq)
            })
      } else {
        console.log('Received error. Error will be sent back instead of results - ' + errorResourceReq)
        reject(errorResourceReq)
      }
    } else {
      var entries = results.data.entries
      console.log(JSON.stringify(entries[0]))
      resolve(entries[0])
    }
  })
}

var __accessToken = null

// get wrapper access token
var getWrapperAccessToken = () => {
  if (__accessToken && (Math.floor(Date.now() / 1000) - wrapper.authorizedTimestamp) < 3600) {
    return Promise.resolve(__accessToken)
  }

  return new Promise(function (resolve, reject) {
    wrapper.auth(function (error, authResults) {
      if (error) {
        console.log('Error occurred while getting access token')
        console.log(error, error.stack)
        reject(error)
      }
      __accessToken = wrapper.authorizedToken
      resolve(wrapper.authorizedToken)
    })
  })
}
