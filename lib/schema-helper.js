const config = require('config')
const request = require('request');
//get schema

exports.schema = function(schema_type) {
  return new Promise(function(resolve, reject) {
      var schema_base_url = null
      if(!(config.get('schema_base_url').endsWith('/'))){
        schema_base_url = config.get('schema_base_url').concat('/')
      }
      var url = schema_base_url.concat(schema_type)
      console.log('Querying for schema - ' + url);
      request(url, function(error, response, body) {
        if(!error && response.statusCode == 200){
          resolve(JSON.parse(body).data.schema);
        } else {
          console.log("An error occurred - " + response.statusCode);
          reject("Error occurred while getting schema - " + response.statusCode);
        }
      })
    })
}