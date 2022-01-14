
const avro = require('avsc')
const wrapper = require('@nypl/sierra-wrapper')
const config = require('config')
const retry = require('retry')
const bunyan = require('bunyan')
const schemaHelper = require('./lib/schema-helper')
const async = require('async');

var streams = require('./lib/stream')

const SCHEMAREADINGSTREAM = 'schemaReadingStream'
const NYPLSOURCE = 'sierra-nypl'
const PARALLEL_LIMIT = parseInt(process.env.PARALLEL_LIMIT) || 3

var __accessToken = null

const isABib = process.env.RETRIEVAL_TYPE === 'bib'
var log = isABib ? bunyan.createLogger({name: 'sierra-bib-retriever'}) : bunyan.createLogger({name: 'sierra-item-retriever'})

log.info('Loading sierra resource retriever service')

// main function
exports.handler = function (event, context, callback) {
  var record = event.Records[0]
  if (record.kinesis) {
    kinesisHandler(event.Records, context, callback)
  }
}

// kinesis stream handler
var kinesisHandler = function (records, context, callback) {
  log.info('Processing ' + records.length + ' records')

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
    // Now we're oauthed and have parsed schemas, so get the marc in json from sierra:
    .then((schemas) => {
      return getDetailedResource(records, schemas)
    })
    .then((results) => {
      var resultsForStream = []
      results.forEach((entry) => {
        if (entry.error) {
          callback(entry.error, `Failed to get marc in json for ${entry.record}`)
        } else if (entry.record.id) {
          resultsForStream.push(entry.record)
        }
      })
      return resultsForStream
    })
    // Now post to kinesis
    .then((marcInJsonRecords) => {
      if (isABib) { return postToStream(marcInJsonRecords, config.get('bib.streamToPost'), config.get('bib.schemaPostingStream')) } else { return postToStream(marcInJsonRecords, config.get('item.streamToPost'), config.get('item.schemaPostingStream')) }
    })
    // Now tell the lambda enviroment whether there was an error or not:
    .then((resultPosted) => {
      log.info('Completed processing records - ', resultPosted.records.length)
    })
    .catch((error) => {
      var errorDetail = {'message': error, 'details': error}
      log.error({APP_ERROR: errorDetail})
      log.error('calling callback() with error')
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
  if (isABib) {
    schemaReadingStream = config.get('bib.schemaReadingStream')
  } else {
    schemaReadingStream = config.get('item.schemaReadingStream')
  }
  return Promise.all([
    schemaHelper.schema(schemaReadingStream)
  ])
    .then((allSchemas) => {
      log.debug('Sending all schemas')
      CACHE[SCHEMAREADINGSTREAM] = allSchemas[0]
      return Promise.resolve(CACHE)
    })
    .catch((error) => {
      log.error('Error occurred while getting schemas')
      return Promise.reject(error)
    })
}

// process resources
var getDetailedResource = (records, schemas) => {
  var asyncTasks = [];

  records.map((record) => {
    var data = record.kinesis.data
    var jsonData = avroDecodedData(schemas[SCHEMAREADINGSTREAM], data)

    if (isABib) {
      asyncTasks.push((callback) => {
        return resourceInDetail(jsonData.id, true)
          .then((sierraResource) => {
            if (sierraResource) {
              var bib = sierraResource
              bib['nyplSource'] = NYPLSOURCE
              bib['nyplType'] = 'bib'

              callback(null, { error: null, record: bib })
            } else {
              log.error(`No valid bib obtained for ${jsonData.id}`)
              callback(null, { error: null, record: jsonData.id })
            }
          })
          .catch((e) => {
            log.error('Error with bib: ' + jsonData.id, e)
            callback({ error: e, record: jsonData.id })
          })
      })
    } else {
      asyncTasks.push((callback) => {
        return resourceInDetail(jsonData.id, false)
          .then((sierraResource) => {
            if (sierraResource) {
              var item = sierraResource
              item['nyplSource'] = NYPLSOURCE
              item['nyplType'] = 'item'
              callback(null, { error: null, record: item })
            } else {
              log.error(`No valid item obtained for ${jsonData.id}`)
              callback(null, {error: null, record: jsonData.id})
            }
          })
          .catch((e) => {
            log.error('Error with item: ' + jsonData.id, e)
            callback({ error: e, record: jsonData.id })
          })
      })
    }
  })

  return new Promise((resolve, reject) => {
    async.parallelLimit(asyncTasks, PARALLEL_LIMIT, (error, results) => {
      if (error) {
        reject(error)
      }
      resolve(results)
    })
  })
}

var postToStream = (records, stream, schemaName) => {
  return new Promise((resolve, reject) => {
    streams.streamPoster(records, stream, schemaName)
        .then(() => {
          resolve({error: null, records: records})
        })
        .catch((error) => {
          log.error('Error occurred posting to kinesis - ' + error)
          reject({error: error, records: records})
        })
  })
}

// use avro to deserialize
var avroDecodedData = (schemaData, record) => {
  const type = avro.parse(schemaData)
  var decoded = Buffer.from(record, 'base64')
  var verify = type.fromBuffer(decoded)
  return JSON.parse(verify)
}

// get full bib/item details for each bib/item id
var resourceInDetail = (id, isBib) => {
  return new Promise((resolve, reject) => {
    var operation = retry.operation({
      retries: 5,
      factor: 3,
      minTimeout: 1 * 1000,
      maxTimeout: 60 * 1000,
      randomize: true
    })
    operation.attempt((currentAttempt) => {
      if (isBib) {
        log.info(`Requesting for bib ${id}`)
        wrapper.requestSingleBib(id, (errorBibReq, results) => {
          if (errorBibReq) {
            var errorDetail = {message: 'Error occurred while calling sierra api for bib', detail: JSON.parse(errorBibReq)}
            log.error({API_ERROR: errorDetail})
          }
          getResource(errorBibReq, results, true, id, operation, currentAttempt)
            .then((resource) => {
              if (currentAttempt > 1) {
                log.info({ message: `Retry attempt ${currentAttempt} succeeded for bib ${id}` })
              }
              resolve(resource)
            })
              .catch((error) => {
                // If our retry-operation wants to try again, allow it:
                if (operation.retry(error)) {
                  log.info({ message: `Retrying fetch for bib ${id}, attempt ${currentAttempt}` })
                  return
                }
                // Otherwise, we must have exhausted retries, so fail hard:
                else reject(error)
              })
        })
      } else {
        var itemIds = [id]
        log.info(`Requesting for item ${id}`)
        wrapper.requestMultiItemBasic(itemIds, (errorItemReq, results) => {

          if (errorItemReq) {
            let detail = errorItemReq
            // Network errors may produce errors that are not stringified json
            try { detail = JSON.parse(detail) } catch (e) {}
            var errorDetail = {message: 'Error occurred while calling sierra api for item', detail }
            log.error({API_ERROR: errorDetail})
          }
          getResource(errorItemReq, results, false, itemIds, operation, currentAttempt)
            .then((resource) => {
              if (currentAttempt > 1) {
                log.info({ message: `Retry attempt ${currentAttempt} succeeded for item ${id}` })
              }
              resolve(resource)
            })
              .catch((error) => {
                /*
                 * On use of operation.retry:
                 * > Returns false when no error value is given, or the maximum amount of retries has been reached.
                 * > Otherwise it returns true, and retries the operation after the timeout for the current attempt number.
                 */
                // If our retry-operation wants to try again, allow it:
                if (operation.retry(error)) {
                  log.info({ message: `Retrying fetch for item ${id}, attempt ${currentAttempt}` })
                  return
                }
                // Otherwise, we must have exhausted retries, so fail hard:
                else reject(error)
              })
        })
      }
    })
  })
}

var getResource = (errorResourceReq, results, isBib, resourceId, operation, attemptNumber) => {
  return new Promise((resolve, reject) => {
    getResult(errorResourceReq, results, isBib, resourceId, operation, attemptNumber)
            .then((sierraResource) => {
              if (sierraResource != null) {
                resolve(sierraResource.resource)
              } else {
                log.error('No resource returned from results')
                resolve(null)
              }
            })
            .catch(function (e) {
              log.error('Error occurred while getting item - ' + e)
              reject(e)
            })
  })
}

// get bib or item based on switch isBib passed
var getResult = (errorResourceReq, results, isBib, resourceId, operation, attemptNumber) => {
  return new Promise((resolve, reject) => {
    var sierraResource = null
    if (errorResourceReq) {
      var errorInJson = errorResourceReq
      // Network errors may produce errors that are not stringified json
      try { errorInJson = JSON.parse(errorInJson) } catch (e) {}
      log.error('Error httpstatus -' + errorInJson.httpStatus + '-')
      if (errorInJson.httpStatus === 401) {
        __accessToken = null
        log.error('This is a token issue')
        reject(errorResourceReq)
      } else if (errorInJson.httpStatus === 400) {
        log.error({APP_ERROR: errorResourceReq}, 'Bad request sent to retrieve resource')
        resolve(sierraResource)
      } else if (errorInJson.httpStatus === 404) {
        log.error({APP_ERROR: errorResourceReq}, 'Resource not found')
        resolve(sierraResource)
      } else {
        log.error('Received error. Error will be sent back instead of results - ' + errorResourceReq)
        reject(errorResourceReq)
      }
    } else {
      var entry = results.data.entries[0]
      sierraResource = {'resource': entry}
      log.info({entry: sierraResource.resource})
      resolve(sierraResource)
    }
  })
}

// get wrapper access token
var getWrapperAccessToken = () => {
  if (__accessToken) {
    return Promise.resolve(__accessToken)
  }

  return new Promise(function (resolve, reject) {
    wrapper.auth(function (error, authResults) {
      if (error) {
        log.error('Error occurred while getting access token')
        log.error(error, error.stack)
        reject(error)
      }
      __accessToken = wrapper.authorizedToken
      resolve(wrapper.authorizedToken)
    })
  })
}
