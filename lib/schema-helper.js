const config = require('config')
const request = require('request')
const bunyan = require('bunyan')

const isABib = process.env.RETRIEVAL_TYPE === 'bib'
var log = isABib ? bunyan.createLogger({name: 'sierra-bib-retriever'}) : bunyan.createLogger({name: 'sierra-item-retriever'})

// get schema

exports.schema = function (schemaType) {
  return new Promise(function (resolve, reject) {
    var schemaBaseUrl = null
    if (!(config.get('schemaBaseUrl').endsWith('/'))) {
      schemaBaseUrl = config.get('schemaBaseUrl').concat('/')
    }
    var url = schemaBaseUrl.concat(schemaType)
    request(url, function (error, response, body) {
      if (!error && response.statusCode === 200) {
        resolve(JSON.parse(body).data.schema)
      } else {
        log.error('Error occurred while requesting for schema. Response code - ' + response.statusCode)
        reject(response.statusCode)
      }
    })
  })
}
