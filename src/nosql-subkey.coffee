# Copyright (c) 2015 Riceball LEE, MIT License
#xtend                 = require("xtend")
#util                  = require("abstract-object/lib/util")
Errors                = require("abstract-object/Error")
EncodingNoSQL         = require("nosql-encoding")
AbstractNoSQL         = EncodingNoSQL.super_
#AbstractNoSQL         = require("abstract-nosql")
SubkeyIterator        = require("./subkey-iterator")
SubkeyCache           = require("./subkey-cache")
Codec                 = require("buffer-codec")
inherits              = require("inherits-ex/lib/inherits")
inheritsDirectly      = require("inherits-ex/lib/inheritsDirectly")
isInheritedFrom       = require("inherits-ex/lib/isInheritedFrom")
isFunction            = require("util-ex/lib/is/type/function")
isString              = require("util-ex/lib/is/type/string")
isObject              = require("util-ex/lib/is/type/object")
isArray               = require("util-ex/lib/is/type/array")
extend                = require("util-ex/lib/_extend")
hooks                 = require("./hooks")
path                  = require("./path")
codec                 = require("./codec")
consts                = require("./consts")
addPathToRange        = require("./range").addPathToRange
subkey                = require("./subkey")

PATH_SEP              = codec.PATH_SEP
SUBKEY_SEP            = codec.SUBKEY_SEP
lowerBound            = codec.lowerBound
upperBound            = codec.upperBound
resolveKeyPath        = codec.resolveKeyPath
prepareKeyPath        = codec.prepareKeyPath
prepareOperation      = codec.prepareOperation
_encodeKey            = codec._encodeKey
encodeKey             = codec.encodeKey
decodeKey             = codec.decodeKey
#encode                = codec.encode
#decode                = codec.decode
getPathArray          = codec.getPathArray
#inheritsDirectly      = util.inheritsDirectly
#isArray               = util.isArray
InvalidArgumentError  = Errors.InvalidArgumentError
WriteError            = Errors.WriteError

toPath                = path.join
relativePath          = path.relative
resolvePathArray      = path.resolveArray


module.exports = class SubkeyNoSQL
  inherits SubkeyNoSQL, EncodingNoSQL

  # Data Operation Type:
  @GET_OP   = GET_OP  = consts.GET_OP
  @PUT_OP   = PUT_OP  = consts.PUT_OP
  @DEL_OP   = DEL_OP  = consts.DEL_OP
  @TRANS_OP = TRANS_OP= consts.TRANS_OP
  @HALT_OP  = HALT_OP = consts.HALT_OP
  @SKIP_OP  = SKIP_OP = consts.SKIP_OP

  constructor: (aClass)->
    if (this not instanceof SubkeyNoSQL)
      if isInheritedFrom aClass, 'SubkeyNoSQL'
        #throw new InvalidArgumentError("this class has already been subkey-able.")
        console.error aClass.name + " class has already been subkey-able."
        return aClass
      vParentClass = isInheritedFrom aClass, EncodingNoSQL if EncodingNoSQL
      vParentClass = isInheritedFrom aClass, 'AbstractNoSQL' unless vParentClass
      if vParentClass
        inheritsDirectly vParentClass, SubkeyNoSQL
        vIteratorClass = aClass::IteratorClass
        if vIteratorClass and not isInheritedFrom vIteratorClass, 'SubkeyIterator'
          if vIt = isInheritedFrom vIteratorClass, 'EncodingIterator'
            vIteratorClass = vIt
          else
            vIteratorClass = isInheritedFrom vIteratorClass, 'AbstractIterator'
          inheritsDirectly vIteratorClass, SubkeyIterator if vIteratorClass
        return aClass
      else
        throw new InvalidArgumentError("class should be inherited from EncodingNoSQL or AbstractNoSQL")
    AbstractNoSQL::constructor.apply(this, arguments)
  initialize: ->
    @cache = new SubkeyCache()
    @SubkeyClass = subkey(@)
    super
  finalize: ->
    @cache.free()
    @cache = undefined
    @SubkeyClass = undefined
    super
  ###
    first check preHooks if operationType
    return falsse if prehooks disallow this operation 
    or return encoded string.
  ###
  encodeKey: (aPathArray, aKey, op, operationType)->
    result = prepareOperation(@preHooks, operationType, op, aPathArray, aKey)
    return false if result is HALT_OP

    keyEncoding = @keyEncoding op
    if result is SKIP_OP
      #skip path wrapped key, just encode aKey directly.
      aKey = keyEncoding.encode aKey if keyEncoding
      return aKey 
    _encodeKey op.path, op.key, keyEncoding, op
  decodeKey: (key, options)->
    keyEncoding = @keyEncoding options
    decodeKey key, keyEncoding, options
  setOpened: (isOpened, options)->
    if isOpened
      @preHooks = hooks()
      @postHooks = hooks()
      @cache.reset(options)
      if options and options.path
        @_pathArray = getPathArray options.path
        delete options.path
      else
        @_pathArray = []
    else
      @preHooks.free() if @preHooks
      @postHooks.free() if @postHooks
      @cache.clear()
      @preHooks = null
      @postHooks = null
      @_pathArray = null
    super(isOpened, options)
  getPathArray: (options, aParentPath) ->
    vRootPath = @_pathArray
    vRootPath = getPathArray(aParentPath, vRootPath) if aParentPath
    result = if options and options.path then getPathArray(options.path, vRootPath) else vRootPath
    result
  isExistsSync: (key, options) ->
    path = @getPathArray(options)
    options = extend {}, @_options, options
    key = @encodeKey(path, key, options)
    return false if key is false
    AbstractNoSQL::isExistsSync.call @, key, options
  isExistsAsync: (key, options, callback) ->
    path = @getPathArray(options)
    if isFunction options
      callback = options
      options = undefined
    options = extend {}, @_options, options
    key = @encodeKey(path, key, options)
    return false if key is false
    AbstractNoSQL::isExistsAsync.call @, key, options, callback
  getSync: (key, options) ->
    path = @getPathArray(options)
    options = extend {}, @_options, options
    key = @encodeKey path, key, options, GET_OP
    return false if key is false
    result = AbstractNoSQL::getSync.call(@, key, options)
    if result isnt undefined
      encoding = @valueEncoding options
      result = encoding.decode(result) if encoding
    result
  getAsync: (key, options, callback) ->
    path = @getPathArray(options)
    if isFunction options
      callback = options
      options = undefined
    options = extend {}, @_options, options
    key = @encodeKey(path, key, options, GET_OP)
    return false if key is false
    encoding = @valueEncoding options
    AbstractNoSQL::getAsync.call @, key, options, (err, value)=>
      return @dispatchError err, callback if err
      value = encoding.decode(value) if encoding
      callback null, value
  getBufferSync: (key, destBuffer, options) ->
    path = @getPathArray(options)
    options = extend {}, @_options, options
    key = @encodeKey path, key, options, GET_OP
    return false if key is false
    AbstractNoSQL::getBufferSync.call(@, key, destBuffer, options)
  getBufferAsync: (key, destBuffer, options, callback) ->
    path = @getPathArray(options)
    if isFunction options
      callback = options
      options = undefined
    options = extend {}, @_options, options
    key = @encodeKey(path, key, options, GET_OP)
    return false if key is false
    AbstractNoSQL::getBufferAsync.call(@, key, destBuffer, options, callback)
  mGetSync: (keys, options) ->
    path = @getPathArray(options)
    options = extend {}, @_options, options
    valueEncoding = @valueEncoding options
    result = []
    vOptions = extend {}, options
    for k in keys
      k = @encodeKey path, k, vOptions, GET_OP
      vOptions.path = options.path
      result.push k if k isnt false
        
    result = AbstractNoSQL::mGetSync.call(@, result, options)
    if options.keys isnt false
      result.map (item)=>
        item.key = @decodeKey item.key, options
        item.value = valueEncoding.decode item.value if valueEncoding
    else
      result = result.map valueEncoding.decode.bind(valueEncoding) if valueEncoding
    result
  mGetAsync: (keys, options, callback) ->
    path = @getPathArray(options)
    if isFunction options
      callback = options
      options = undefined
    options = extend {}, @_options, options
    valueEncoding = @valueEncoding options
    vKeys = []
    vOptions = extend {}, options
    for k in keys
      k = @encodeKey path, k, vOptions, GET_OP
      vOptions.path = options.path
      vKeys.push k if k isnt false
    that = @
    AbstractNoSQL::mGetAsync.call @, vKeys, options, (err, result)->
      return that.dispatchError err, callback if err
      if options.keys isnt false
        result.map (item)->
          item.key = that.decodeKey item.key, options
          item.value = valueEncoding.decode item.value if valueEncoding
      else
        result = result.map valueEncoding.decode.bind(valueEncoding) if valueEncoding
      callback null, result
  putSync: (key, value, options) ->
    path = @getPathArray(options)
    options = extend {}, @_options, options
    key = @encodeKey path, key, options, PUT_OP
    return false if key is false
    valueEncoding = @valueEncoding options
    value = valueEncoding.encode(value) if valueEncoding
    result = AbstractNoSQL::putSync.call(@, key, value, options)
    if result
      vKeyPath = options._keyPath
      options.path = vKeyPath[0]
      options.key = vKeyPath[1]
      delete options._keyPath
      @postHooks.trigger PUT_OP, vKeyPath, [PUT_OP, options]
    result
  putAsync: (key, value, options, callback) ->
    path = @getPathArray(options)
    if isFunction options
      callback = options
      options = undefined
    options = extend {}, @_options, options
    key = @encodeKey path, key, options, PUT_OP
    return false if key is false
    valueEncoding = @valueEncoding options
    value = valueEncoding.encode(value) if valueEncoding
    AbstractNoSQL::putAsync.call @, key, value, options, (err, result)=>
      return @dispatchError err, callback if err
      vKeyPath = options._keyPath
      options.path = vKeyPath[0]
      options.key = vKeyPath[1]
      delete options._keyPath
      @postHooks.trigger PUT_OP, vKeyPath, [PUT_OP, options]
      callback(null, result) if callback
  delSync: (key, options) ->
    path = @getPathArray(options)
    options = extend {}, @_options, options
    key = @encodeKey path, key, options, DEL_OP
    return false if key is false
    result = AbstractNoSQL::delSync.call(@, key, options)
    if result
      vKeyPath = options._keyPath
      options.path = vKeyPath[0]
      options.key = vKeyPath[1]
      delete options._keyPath
      @postHooks.trigger DEL_OP, vKeyPath, [DEL_OP, options]
    result
  delAsync: (key, options, callback) ->
    path = @getPathArray(options)
    if isFunction options
      callback = options
      options = undefined
    options = extend {}, @_options, options
    key = @encodeKey path, key, options, DEL_OP
    return false if key is false
    AbstractNoSQL::delAsync.call @, key, options, (err, result)=>
      return @dispatchError err, callback if err
      vKeyPath = options._keyPath
      options.path = vKeyPath[0]
      options.key = vKeyPath[1]
      delete options._keyPath
      @postHooks.trigger DEL_OP, vKeyPath, [DEL_OP, options]
      callback(null, result) if callback
  prepareOperations: (operations, options)->
    keyEncoding = @keyEncoding options
    valueEncoding = @valueEncoding options
    i = 0
    vParentPath = options.path if options
    #apply prehooks here.
    while i < operations.length
      op = operations[i]
      op.type = 'put' unless op.type
      op.path = @getPathArray(op, vParentPath)
      op.separator = options.separator if not op.separator and options and options.separator
      result = prepareOperation(@preHooks, TRANS_OP, op)
      if result is HALT_OP
        delete operations[i]
      else if result isnt SKIP_OP # skip encodeKey
        op.key = _encodeKey op.path, op.key, Codec(op.keyEncoding) || keyEncoding, op
      vEncoding = Codec(op.valueEncoding) || valueEncoding
      op._keyPath[2] = op.value
      op.value = vEncoding.encode(op.value) if vEncoding
      i++
    operations
  batchSync: (operations, options) ->
    if isArray operations
      @prepareOperations operations, options
      result = AbstractNoSQL::batchSync.call(@, operations, options)
      if result
        operations.forEach (op) =>
          vKeyPath = op._keyPath
          op.path = vKeyPath[0]
          op.key = vKeyPath[1]
          op.value = vKeyPath[2]
          delete op._keyPath
          vOpType = if op.type is 'del' then DEL_OP else PUT_OP
          @postHooks.trigger(vOpType, vKeyPath, [vOpType, op]) if op.triggerAfter != false
      result
    else
      AbstractNoSQL::batchSync.call(@, operations, options)
  batchAsync: (operations, options, callback) ->
    if isFunction options
      callback = options
      options = undefined
    if isArray operations
      @prepareOperations operations, options
      that = @
      AbstractNoSQL::batchAsync.call @, operations, options, (err, result)->
        return that.dispatchError err, callback if err
        operations.forEach (op) ->
          vKeyPath = op._keyPath
          op.path = vKeyPath[0]
          op.key = vKeyPath[1]
          op.value = vKeyPath[2]
          delete op._keyPath
          vOpType = if op.type is 'del' then DEL_OP else PUT_OP
          that.postHooks.trigger(vOpType, vKeyPath, [vOpType, op]) if op.triggerAfter != false
        callback err, result if callback
    else
      AbstractNoSQL::batchAsync.call @, operations, options, callback
    return
  #TODO: approximateSizeSync should not be here.
  approximateSizeSync:(start, end, options) ->
    path = @getPathArray(options)
    keyEncoding = @keyEncoding()
    options = extend {}, @_options, options
    vOptions = extend {}, options
    start = encodeKey(path, start, keyEncoding, options) if start isnt undefined
    vOptions.path = options.path
    end = encodeKey(path, end, keyEncoding, options) if end isnt undefined
    AbstractNoSQL::approximateSizeSync.call(@, start, end)
  #TODO: approximateSizeAsync should not be here.
  approximateSizeAsync:(start, end, callback) ->
    path = @getPathArray(options)
    keyEncoding = @keyEncoding()
    options = extend {}, @_options, options
    vOptions = extend {}, options
    start = encodeKey(path, start, keyEncoding, options) if start isnt undefined
    vOptions.path = options.path
    end = encodeKey(path, end, keyEncoding, options) if end isnt undefined
    AbstractNoSQL::approximateSizeAsync.call(@, start, end, callback)
  iterator: (options) ->
    path = @getPathArray(options)
    options = extend {}, @_options, options
    options.path = path
    super(options)
  _addHookTo: (hooks, opType, range, path, callback)->
    if @_options.path
      path = resolvePathArray @_options.path, path
      path.shift(0,1)
    if isFunction(range)
      callback = range
      range = [path]
    else if isString(range)
      range = resolveKeyPath(path, range)
    else if isObject(range)
      range = addPathToRange(path, range)
    else
      #TODO: handle ranges, needed for level-live-stream, etc.
      throw new Error("not implemented yet")

    hooks.add opType, range, callback
  pre: (opType, range, path, callback)->
    @_addHookTo @preHooks, opType, range, path, callback
  post: (opType, range, path, callback)->
    @_addHookTo @postHooks, opType, range, path, callback

  createSubkey: (aKeyPath, aOptions, aReadyCallback)->
    if isFunction aOptions
      aReadyCallback = aOptions
      aOptions = undefined
    aOptions = extend {}, @_options, aOptions
    @SubkeyClass(aKeyPath, aOptions, aReadyCallback)
  createPath: @::createSubkey
  subkey: (aKeyPath, aOptions, aReadyCallback)->
    if isFunction aOptions
      aReadyCallback = aOptions
      aOptions = undefined
    aOptions = extend {}, @_options, aOptions
    aOptions.addRef = false
    @SubkeyClass(aKeyPath, aOptions, aReadyCallback)
  path: @::subkey

  root: (aOptions, aReadyCallback)->
    @subkey(null, aOptions, aReadyCallback)

