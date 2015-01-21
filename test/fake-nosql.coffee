EncodingNoSQL   = require 'nosql-encoding'
sinon           = require 'sinon'
inherits        = require 'abstract-object/lib/util/inherits'
isObject        = require 'abstract-object/lib/util/isObject'
FakeIterator    = require './fake-iterator'

module.exports = class FakeDB
  inherits FakeDB, EncodingNoSQL
  constructor: ->super
  IteratorClass: FakeIterator
  _openSync: sinon.spy ->
    @data = {}
    true
  _closeSync: sinon.spy ->
    @data = {}
    true
  _isExistsSync: sinon.spy (key)->
    @data.hasOwnProperty key
  _getBufferSync: sinon.spy ->
    EncodingNoSQL::_getBufferSync.apply this, arguments
  _getSync: sinon.spy (key, opts)->
    #@data[key]
    encoding = @valueEncoding opts
    if encoding then encoding.encode(key) else '"'+key+'"'
  _putSync: sinon.spy (key,value)->@data[key]=value
  _delSync: sinon.spy (key)->delete @data[key]
  _batchSync: sinon.spy (operations, options)->
    for op in operations
      continue unless isObject op
      if op.type is 'del'
        delete @data[op.key]
      else
        @data[op.key] = op.value
    true


