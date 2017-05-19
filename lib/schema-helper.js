const config = require('config')
const request = require('request')
const bunyan = require('bunyan')

const isABib = process.env.RETRIEVAL_TYPE === 'bib'
var log = isABib ? bunyan.createLogger({name: 'sierra-bib-retriever'}) : bunyan.createLogger({name: 'sierra-item-retriever'})

// get schema

exports.schema = (schemaType) => {
  return new Promise((resolve, reject) => {
    var schemaBaseUrl = null
    if (!(config.get('schemaBaseUrl').endsWith('/'))) {
      schemaBaseUrl = config.get('schemaBaseUrl').concat('/')
    }
    var url = schemaBaseUrl.concat(schemaType)
    request(url, (error, response, body) => {
      if (!error && response.statusCode === 200) {
        resolve(JSON.parse(body).data.schema)
      } else {
        var schemaResponse = {}
        schemaResponse.statusCode = response != null ? response.statusCode : null
        schemaResponse.body = body != null ? body : null
        schemaResponse.error = error != null ? error : null
        log.error({AvroSchemaRetrievalError: schemaResponse}, 'Error occurred while retrieving schema')
        error === null ? reject('api-response: ' + response.statusCode) : reject(error)
      }
    })
  })
}
