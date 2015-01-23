EncodingNoSQL   = require 'nosql-encoding'
sinon           = require 'sinon'
inherits        = require 'abstract-object/lib/util/inherits'
isObject        = require 'abstract-object/lib/util/isObject'
errors          = require 'abstract-object/Error'
FakeIterator    = require './fake-iterator'


NotFoundError   = errors.NotFoundError

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
  _mGetSync: sinon.spy ->
    EncodingNoSQL::_mGetSync.apply this, arguments
  _getBufferSync: sinon.spy ->
    EncodingNoSQL::_getBufferSync.apply this, arguments
  _getSync: sinon.spy (key, opts)->
    #encoding = @valueEncoding opts
    #if encoding then encoding.encode(key) else '"'+key+'"'
    if @data.hasOwnProperty key
      @data[key]
    else
      throw new NotFoundError("NotFound:"+key)
  _putSync: sinon.spy (key,value)->
    @data[key]=value
  _delSync: sinon.spy (key)->delete @data[key]
  _batchSync: sinon.spy (operations, options)->
    for op in operations
      continue unless isObject op
      if op.type is 'del'
        delete @data[op.key]
      else
        @data[op.key] = op.value
    true


