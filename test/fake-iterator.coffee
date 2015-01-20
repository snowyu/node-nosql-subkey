sinon             = require 'sinon'
EncodingIterator  = require 'encoding-iterator'
inherits          = require 'abstract-object/lib/util/inherits'

module.exports =  class FakeIterator
  inherits FakeIterator, EncodingIterator
  constructor: ->
    @i=0
    super
    @keys = Object.keys @db.data
  _nextSync: sinon.spy ->
    result  = i < @keys.length
    if result
      key   = @keys[i]
      result= [key, @db.data[key]]
      ++i
    result

