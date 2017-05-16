const wrapper = require('@nypl/sierra-wrapper')
const _ = require('highland')

/**
* Request an array of bibs from sierra - use this one, it will try to break down the list and do each individually if the large get fails
*
* @param  {array} bibs - array of bibs id strings
* @param  {function} callback - the callback with (error, results) results is array of records
*/
exports.requestBibs = function (bibs, callback) {
  reqBib(bibs, (error, results) => {
    if (!error) {
      // it works, return
      if (results && results.data && results.data.entries) {
        results = results.data.entries
      }
      callback(error, results)
    } else {
      console.error('Bib request error:', error)
      console.log('Trying each request individually')
      var results = []
      // try each bib on it's own and aggergate
      _(bibs)
        .map(_.curry(reqBib))
        .nfcall([])
        .series()
        .map((result) => {
          if (result && result.data && result.data.entries) {
            result.data.entries.forEach((bib) => {
              results.push(bib)
            })
          } else {
            console.error('Bib request error:', results)
          }
        })
        .done(() => {
          callback(null, results)
        })
    }
  })
}

/**
* Auth helper function
*
* @param  {function} callback - the callback with (error, authToken)
*/
const authorize = function (callback) {
  if (!wrapper.authorizedToken) {
    wrapper.auth((error, results) => {
      callback(error, wrapper.authorizedToken)
    })
  } else {
    callback(null, wrapper.authorizedToken)
  }
}

/**
* requestBibs helper function
*
* @param  {array} bibs - the the array of bib ids to use
* @param  {function} callback - the callback with (error, authToken)
*/
const reqBib = function (bibs, callback) {
  // if an array wans't past make a little one
  if (!Array.isArray(bibs)) bibs = [bibs]
  authorize((errorAuth, authToken) => {
    if (errorAuth) {
      callback(errorAuth, false)
      return false
    }
    // we are okay, lets make the request
    wrapper.requestMultiBibBasic(bibs, (errorBibRequest, results) => {
      callback(errorBibRequest, results)
    })
  })
}
