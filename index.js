
const avro = require('avsc')
const wrapper = require('@nypl/sierra-wrapper')
const config = require('config')
const retry = require('retry')
const bunyan = require('bunyan')
const schemaHelper = require('./lib/schema-helper')
var streams = require('./lib/stream')

const SCHEMAREADINGSTREAM = 'schemaReadingStream'
const SCHEMAPOSTINGSTREAM = 'schemaPostingStream'
const NYPLSOURCE = 'sierra-nypl'

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
      if (failures > 0) {
        error = `${failures} failed`
        var errorDetail = {'source': 'APP_ERROR', 'details': error}
        log.error({APP_ERROR: errorDetail})
      }
      callback(error, `Wrote ${records.length}; Succeeded: ${successes} Failures: ${failures}`)
    })
    .catch((error) => {
      var errorDetail = {'source': 'APP_ERROR', 'details': error}
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
  var schemaPostingStream = null
  if (isABib) {
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
        log.debug('Sending all schemas')
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
  return Promise.all(
    records.map((record) => {
      var data = record.kinesis.data
      var jsonData = avroDecodedData(schemas[SCHEMAREADINGSTREAM], data)
      if (isABib) {
        return resourceInDetail(jsonData.id, true)
          .then((sierraResource) => {
            if(sierraResource != null) {
              var bib = sierraResource
              bib['nyplSource'] = NYPLSOURCE
              bib['nyplType'] = 'bib'
              var stream = config.get('bib.stream')
              return streams.postResourcesToStream(bib, stream, schemas[SCHEMAPOSTINGSTREAM])
                .then(() => ({ error: null, bib: bib }))
                .catch((e) => {
                  log.error('Error occurred posting to kinesis - ' + e)
                  return ({ error: e, bib: bib })
                })
            } else{
              return Promise.resolve(() => {
                log.error('No valid bib obtained')
              })
            }
          })
          .catch((e) => {
            log.error('Error with bib: ' + jsonData.id, e)
            return { error: e, bib: jsonData.id }
          })
      } else {
        return resourceInDetail(jsonData.id, false)
          .then( (sierraResource) => {
            if(sierraResource != null) {
              var item = sierraResource
              item['nyplSource'] = NYPLSOURCE
              item['nyplType'] = 'item'
              var stream = config.get('item.stream')
              return streams.postResourcesToStream(item, stream, schemas[SCHEMAPOSTINGSTREAM])
                .then(() => ({ error: null, item }))
                .catch((e) => {
                  log.error('Error occurred posting to kinesis - ' + e)
                  return ({ error: e, item: item })
                })
            } else {
              return Promise.resolve(() => {
                log.error('No valid item obtained')
              })
            }
          })
          .catch((e) => {
            log.error('Error with item: ', e)
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
        log.info(`Requesting for bib ${id}`)
        wrapper.requestSingleBib(id, (errorBibReq, results) => {
          if (errorBibReq) {
            var errorDetail = {message: 'Error occurred while calling sierra api for bib', detail: JSON.parse(errorBibReq)}
            log.error({API_ERROR: errorDetail})
          }
          getResult(errorBibReq, results, true, id, operation, currentAttempt)
              .then((sierraResource) => {
                if(sierraResource != null) {
                  log.info({entry: sierraResource.resource})
                  resolve(sierraResource.resource)
                } else {
                  log.error('No resource returned from results')
                  resolve(null)
                }
              })
              .catch(function (e) {
                log.error('Error occurred while getting bib - ' + e)
                reject(e)
              })
        })
      } else {
        log.info('Requesting for item info')
        var itemIds = [id]
        wrapper.requestMultiItemBasic(itemIds, (errorItemReq, results) => {
          if (errorItemReq) {
            var errorDetail = {message: 'Error occurred while calling sierra api for item', detail: JSON.parse(errorItemReq)}
            log.error({API_ERROR: errorDetail})
          }
          getResult(errorItemReq, results, false, itemIds, operation, currentAttempt)
            .then((sierraResource) => {
              if(sierraResource != null){
                log.info({entry: sierraResource.resource})
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
    })
  })
}

// get bib or item based on switch isBib passed
var getResult = function (errorResourceReq, results, isBib, resourceId, operation, attemptNumber) {
  return new Promise(function (resolve, reject) {
    var sierraResource = null
    if (errorResourceReq) {
      var errorInJson = JSON.parse(errorResourceReq)
      log.error('Error httpstatus -' + errorInJson.httpStatus + '-')
      if (errorInJson.httpStatus === 401) {
        log.error('This is a token issue. Going to renew token')
        if (isBib) {
          log.info('Number of attempts made for bib ' + resourceId + ' - ' + attemptNumber)
        } else {
          log.info('Number of attempts made for item ' + resourceId + ' - ' + attemptNumber)
        }
        getWrapperAccessToken()
            .then(function (accessToken) {
              if (operation.retry(errorResourceReq)) {
                return
              }
              if (isBib) {
                log.error('Error occurred while getting bib info')
              } else {
                log.error('Error occurred while getting item info')
              }
              reject(errorResourceReq)
            })
      } else if(errorInJson.httpStatus === 400) {
          log.error('Bad request sent to retrieve resource - ' + errorResourceReq)
          resolve(sierraResource)
        } else if(errorInJson.httpStatus === 404) {
          log.error('Resource not found - ' + errorResourceReq)
          resolve(sierraResource)
        } else {
        log.error('Received error. Error will be sent back instead of results - ' + errorResourceReq)
        reject(errorResourceReq)
      }
    } else {
      var entry = results.data.entries[0]
      sierraResource = {'resource' : entry}
      log.info({entry: sierraResource.resource})
      resolve(sierraResource)
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
        log.error('Error occurred while getting access token')
        log.error(error, error.stack)
        reject(error)
      }
      __accessToken = wrapper.authorizedToken
      resolve(wrapper.authorizedToken)
    })
  })
}
