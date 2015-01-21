# Copyright (c) 2015 Riceball LEE, MIT License
ltgt                  = require('ltgt')
EncodingIterator      = require("encoding-iterator")
inherits              = require("abstract-object/lib/util/inherits")
isArray               = require("abstract-object/lib/util/isArray")
extend                = require("abstract-object/lib/util/_extend")
Codec                 = require('buffer-codec')
codec                 = require("./codec")
consts                = require("./consts")

lowerBound            = codec.lowerBound
upperBound            = codec.upperBound
encodeKey             = codec.encodeKey
decodeKey             = codec.decodeKey
toLtgt                = ltgt.toLtgt
GET_OP                = consts.GET_OP

module.exports = class SubkeyIterator
  inherits SubkeyIterator, EncodingIterator

  encodeOptions: (options)->
    vOptions = extend {}, options
    vPath = options.path || []
    options.valueEncoding = Codec(options.valueEncoding)
    options.keyEncoding = keyEncoding = Codec(options.keyEncoding)
    #the key is lowerBound or upperBound.
    #if opts.start is exists then lowBound key is opt.start
    encodeKeyPath = (key) ->
      encoding = if key is lowerBound or key is upperBound then null else keyEncoding
      vOptions.path = options.path
      encodeKey(vPath, key, encoding, vOptions)

    #convert the lower/upper bounds to real lower/upper bounds.
    #codec.lowerBound, codec.upperBound are default bounds in case of the options have no bounds.
    toLtgt(options, options, encodeKeyPath, lowerBound, upperBound) if options.bounded isnt false
    options.keyAsBuffer = options.valueAsBuffer = false
    if isArray options.range
      result = []
      for k in options.range
        vOptions.path = options.path
        k = encodeKey vPath, k, keyEncoding, vOptions, GET_OP
        result.push k if k isnt false
      options.range = result
    #console.log("it:opts:", options)
      
    options
  decodeResult: (result)->
    keyEncoding = @options.keyEncoding
    valueEncoding = @options.valueEncoding
    result[0] = decodeKey result[0], keyEncoding, @options if result[0]?
    result[1] = valueEncoding.decode(result[1], @options) if result[1]? and valueEncoding
    result

