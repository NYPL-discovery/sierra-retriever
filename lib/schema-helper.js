const config = require('config')
const request = require('request')

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
        reject(response.statusCode)
      }
    })
  })
}
