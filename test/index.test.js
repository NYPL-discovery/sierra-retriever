/* global describe, it */

var should = require('should') // eslint-disable-line
var wrapper = require('../lib/api.js')

// make some random bib numbers
var someBibs = []
for (var i =0; i<25;i++){
  var min = Math.ceil(10000000);
  var max = Math.floor(20000000);
  someBibs.push((Math.floor(Math.random() * (max - min)) + min).toString())
}

describe('Tests Bib retriveal', function () {
  this.timeout(150000)
  it('Request 25 bibs', (done) => {
    wrapper.requestBibs(someBibs,(error,results) =>{
      results.length.should.equal(25)
      done()
    })
  })
})
