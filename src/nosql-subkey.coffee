# Copyright (c) 2015 Riceball LEE, MIT License
#xtend                 = require("xtend")
#util                  = require("abstract-object/lib/util")
minimatch             = require('minimatch')
ltgt                  = require('ltgt')
Errors                = require("abstract-object/Error")
AbstractNoSQL         = require("abstract-nosql")
EncodingNoSQL         = require("nosql-encoding")
SecondaryCache        = require("secondary-cache")
Codec                 = require("buffer-codec")
try
  EncodingIterator    = require("encoding-iterator")
  AbstractIterator    = EncodingIterator.super_
inherits              = require("abstract-object/lib/util/inherits")
isInheritedFrom       = require("abstract-object/lib/util/isInheritedFrom")
isFunction            = require("abstract-object/lib/util/isFunction")
isString              = require("abstract-object/lib/util/isString")
isObject              = require("abstract-object/lib/util/isObject")
extend                = require("abstract-object/lib/util/_extend")
hooks                 = require("./hooks")
path                  = require("./path")
codec                 = require("./codec")
consts                = require("./consts")
addPathToRange        = require("./range").addPathToRange
subkey                = require("./subkey")

PATH_SEP              = codec.PATH_SEP
SUBKEY_SEP            = codec.SUBKEY_SEP
#inheritsDirectly      = util.inheritsDirectly
#isArray               = util.isArray
InvalidArgumentError  = Errors.InvalidArgumentError
WriteError            = Errors.WriteError

toPath                = path.join
relativePath          = path.relative
resolvePathArray      = path.resolveArray
encode                = codec.encode
decode                = codec._decode
getPathArray          = codec.getPathArray
toLtgt                = ltgt.toLtgt

class SubkeyCache
  inherits SubkeyCache, SecondaryCache

  constructor: -> super
  createSubkey: (keyPath, Subkey, options, callback) ->
    if options && options.forceCreate == true
      result = new Subkey(options, callback)
    else
      result = @get keyPath
      if result
        result.addRef() if !options || options.addRef != false
        callback(null, result) if callback
      else
        result = new Subkey(options, callback)
        @set keyPath, result, options
        result.on "destroyed", (item) =>
          @del keyPath
    result
  subkeys: (aKeyPattern)->
    result = {}
    if aKeyPattern
      @forEach (v,k)-> result[k] = v if minimatch(k, aKeyPattern)
    else
      @forEach (v,k)-> result[k] = v
    result


module.exports = class SubkeyNoSQL
  inherits SubkeyNoSQL, AbstractNoSQL

  addEncodings = (op, aParent) ->
    if aParent && aParent._options
      op.keyEncoding ||= aParent._options.keyEncoding
      op.valueEncoding ||= aParent._options.valueEncoding
    op
  resolveKeyPath = (aPathArray, aKey, op)->
    op = {} unless op
    prepareKeyPath(aPathArray, aKey, op)
    [op.path, op.key]
  prepareKeyPath = (aPathArray, aKey, op) ->
    if isString(aKey) && aKey.length
      aPathArray = resolvePathArray(aPathArray, aKey)
      isAbsolutePath = aPathArray.shift(0,1)
      aKey = aPathArray.pop()
      if op.separator && op.separator != PATH_SEP
        aKey = op.separator + aKey if aKey[0] != op.separator
        op.separator = undefined if aKey[0] is op.separator
      op._keyPath = [aPathArray, aKey]
    op.path = aPathArray
    op.key = aKey
    return
  prepareOperation = (operationType, aOperation, aPathArray, aKey)->
    if aOperation.path
      addEncodings(aOperation, aOperation.path) #if aOperation.path is a subkey object.
      aOperation.path = getPathArray(aOperation.path)
    if aPathArray
      aPathArray = resolvePathArray aOperation.path, aPathArray if aOperation.path
    else
      aPathArray = aOperation.path
    aKey = aOperation.key unless aKey
    prepareKeyPath(aPathArray, aKey, aOperation)
    delete aOperation.separator unless aOperation.separator
    if operationType and aOperation.triggerBefore isnt false
      switch operationType
        when PUT_OP, DEL_OP
          triggerArgs = [operationType, aOperation]
        when TRANS_OP
          addOp = (aOperation) ->
            if aOperation
              #aOperation.path = op.path unless aOperation.path
              #prepareOperation(aOperation)
              ops.push(aOperation)
            return
          triggerArgs = [operationType, aOperation, addOp, ops]
      if triggerArgs
        result = preHooks.trigger operationType, triggerArgs
    return result
  _encodeKey = (aPathArray, aKey, keyEncoding, options)->
    aKey = keyEncoding.encode(aKey) if keyEncoding
    if options
      vSep    = options.separator
      vSepRaw = options.separatorRaw
    encode aPathArray, aKey, vSep, vSepRaw

  encodeKey = (aPathArray, aKey, keyEncoding, options, operationType)->
    prepareOperation(operationType, options, aPathArray, aKey)
    aPathArray = options.path
    aKey = options.key
    _encodeKey aPathArray, aKey, keyEncoding, options
  decodeKey = (key, keyEncoding, options)->
    #v=[parent, key, separator, realSeparator]
    #realSeparator is optional only opts.separator && opts.separator != realSeparator
    v = decode(key, options && options.separator)
    vSep = v[2] #separator
    vSep = PATH_SEP unless vSep?  #if the precodec is other codec.
    key = if keyEncoding then keyEncoding.decode(v[1], options) else v[1]
    if options
      if options.absoluteKey
          key = toPath(v[0]) + vSep + key
      else if options.path && isString(key) && key != ""
          vPath = relativePath(options.path, v[0])
          if vPath is "" and vSep is PATH_SEP
            vSep = "" 
          else if vSep.length >= 2
            vSep = vSep.substring(1)
          key = vPath + vSep + key
    else
      key = v[1]
    key

  # Data Operation Type:
  @GET_OP   = GET_OP  = consts.GET_OP
  @PUT_OP   = PUT_OP  = consts.PUT_OP
  @DEL_OP   = DEL_OP  = consts.DEL_OP
  @TRANS_OP = TRANS_OP= consts.TRANS_OP
  @HALT_OP  = HALT_OP = consts.HALT_OP
  @SKIP_OP  = SKIP_OP = consts.SKIP_OP

  constructor: (aClass)->
    if (this not instanceof SubkeyNoSQL)
      vParentClass = isInheritedFrom aClass, EncodingNoSQL
      vParentClass = isInheritedFrom aClass, AbstractNoSQL unless vParentClass
      if vParentClass
        inheritsDirectly vParentClass, SubkeyNoSQL if vParentClass isnt SubkeyNoSQL
        return aClass
      else
        throw new InvalidArgumentError("class should be inherited from EncodingNoSQL or AbstractNoSQL")
    @Subkey = subkey(@)
    super
  keyEncoding: (options)->
    if options and options.keyEncoding
      encoding = options.keyEncoding
      encoding = Codec(encoding) if encoding
    else if @_options
      encoding = @_options.keyEncoding
    encoding
  valueEncoding: (options)->
    if options and options.valueEncoding
      encoding = options.valueEncoding
      encoding = Codec(encoding) if encoding
    else if @_options
      encoding = @_options.valueEncoding
    encoding
  ### 
    first check preHooks if operationType
    return falsse if prehooks disallow this operation 
    or return encoded string.
  ###
  encodeKey: (aPathArray, aKey, op, operationType, ops)->
    result = prepareOperation(operationType, op, aPathArray, aKey)
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
    super(isOpened, options)
    if isOpened
      @preHooks = hooks()
      @postHooks = hooks()
      @cache = new SubkeyCache(options)
    else
      @preHooks.free() if @preHooks
      @postHooks.free() if @postHooks
      @cache.free() if @cache
      @preHooks = null
      @postHooks = null
      @cache = null
  isExistsSync: (path, key, options) ->
    options = extend {}, @_options, options
    key = @encodeKey(path, key, options)
    return false if key is false
    super key, options
  isExistsAsync: (path, key, options, callback) ->
    if isFunction options
      callback = options
      options = undefined
    options = extend {}, @_options, options
    key = @encodeKey(path, key, options)
    super(key, options, callback)
  getSync: (path, key, options) ->
    options = extend {}, @_options, options
    key = @encodeKey path, key, options, GET_OP
    return false if key is false
    result = super(key, options)
    encoding = @valueEncoding options
    result = encoding.decode(result) if encoding
    result
  getAsync: (path, key, options, callback) ->
    if isFunction options
      callback = options
      options = undefined
    options = extend {}, @_options, options
    key = @encodeKey(path, key, options, GET_OP)
    return false if key is false
    encoding = @valueEncoding options
    super key, options, (err, value)->
      return @dispatchError err, callback if err
      value = encoding.decode(value) if encoding
      callback null, value
  getBufferSync: (path, key, destBuffer, options) ->
    options = extend {}, @_options, options
    key = encodeKey path, key, options, GET_OP
    return false if key is false
    super(key, destBuffer, options)
  getBufferAsync: (path, key, destBuffer, options, callback) ->
    if isFunction options
      callback = options
      options = undefined
    options = extend {}, @_options, options
    key = @encodeKey(path, key, options, GET_OP)
    return false if key is false
    super(key, destBuffer, options, callback)
  mGetSync: (path, keys, options) ->
    options = extend {}, @_options, options
    valueEncoding = @valueEncoding options
    result = []
    vOptions = extend {}, options
    for k in keys
      k = @encodeKey path, k, vOptions, GET_OP
      vOptions.path = options.path
      result.push k if k isnt false
        
    result = super(result, options)
    if options.keys isnt false
      result.map (item)=>
        item.key = @decodeKey item.key, options
        item.value = valueEncoding.decode item.value if valueEncoding
    else
      result = result.map valueEncoding.decode.bind(valueEncoding) if valueEncoding
    result
  mGetAsync: (path, keys, options, callback) ->
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
    super vKeys, options, (err, result)->
      return @dispatchError err, callback if err
      if options.keys isnt false
        result.map (item)->
          item.key = that.decodeKey item.key, options
          item.value = valueEncoding.decode item.value if valueEncoding
      else
        result = result.map valueEncoding.decode.bind(valueEncoding) if valueEncoding
      callback null, result
  putSync: (path, key, value, options) ->
    options = extend {}, @_options, options
    key = @encodeKey path, key, options, PUT_OP
    return false if key is false
    valueEncoding = @valueEncoding options
    value = valueEncoding.encode(value) if valueEncoding
    result = super(key, value, options)
    if result
      vKeyPath = options._keyPath
      options.path = vKeyPath[0]
      options.key = vKeyPath[1]
      delete options._keyPath
      @postHooks.trigger PUT_OP, vKeyPath, [PUT_OP, options]
    result
  putAsync: (path, key, value, options, callback) ->
    if isFunction options
      callback = options
      options = undefined
    options = extend {}, @_options, options
    key = @encodeKey path, key, options, PUT_OP
    return false if key is false
    valueEncoding = @valueEncoding options
    value = valueEncoding.encode(value) if valueEncoding
    super key, value, options, (err, result)=>
      return @dispatchError err, callback if err
      vKeyPath = options._keyPath
      options.path = vKeyPath[0]
      options.key = vKeyPath[1]
      delete options._keyPath
      @postHooks.trigger PUT_OP, vKeyPath, [PUT_OP, options]
      callback(null, result) if callback
  delSync: (path, key, options) ->
    options = extend {}, @_options, options
    key = @encodeKey key, path, options, DEL_OP
    return false if key is false
    result = super(key, options)
    if result
      vKeyPath = options._keyPath
      options.path = vKeyPath[0]
      options.key = vKeyPath[1]
      delete options._keyPath
      @postHooks.trigger DEL_OP, vKeyPath, [DEL_OP, options]
    result
  delAsync: (path, key, options, callback) ->
    if isFunction options
      callback = options
      options = undefined
    options = extend {}, @_options, options
    key = @encodeKey key, path, options, DEL_OP
    super key, options, (err, result)=>
      return @dispatchError err, callback if err
      vKeyPath = options._keyPath
      options.path = vKeyPath[0]
      options.key = vKeyPath[1]
      delete options._keyPath
      @postHooks.trigger DEL_OP, vKeyPath, [DEL_OP, options]
      callback(null, result) if callback
  prepareOperations: (operations)->
    keyEncoding = @keyEncoding options
    valueEncoding = @valueEncoding options

    i = 0
    #apply prehooks here.
    while i < operations.length
      op = operations[i]
      result = prepareOperation(TRANS_OP, op)
      op._keyPath = [op.path, vKey] # keep the original key for postHook.
      if result is HALT_OP
        delete operations[i]
      else if result isnt SKIP_OP
        op.key = _encodeKey op.path, op.key, keyEncoding, operations
      op.value = valueEncoding.encode(op.value) if valueEncoding
      i++
    operations
  batchSync: (operations, options) ->
    if isArray operations
      @prepareOperations operations
      result = super(operations, options)
      if result
        operations.forEach (op) =>
          vKeyPath = op._keyPath
          op.path = vKeyPath[0]
          op.key = vKeyPath[1]
          delete op._keyPath
          vOpType = if op.type is 'del' then DEL_OP else PUT_OP
          @postHooks.trigger(vOpType, vKeyPath, [vOpType, op]) if op.triggerAfter != false
      result
  batchAsync: (operations, options, callback) ->
    if isFunction options
      callback = options
      options = undefined
    if isArray operations
      @prepareOperations operations
      that = @
      super operations, options, (err, result)->
        return that.dispatchError err, callback if err
        operations.forEach (op) ->
          vKeyPath = op._keyPath
          op.path = vKeyPath[0]
          op.key = vKeyPath[1]
          delete op._keyPath
          vOpType = if op.type is 'del' then DEL_OP else PUT_OP
          that.postHooks.trigger(vOpType, vKeyPath, [vOpType, op]) if op.triggerAfter != false
        callback err, result if callback
    return
  #TODO: approximateSizeSync should not be here.
  approximateSizeSync:(path, start, end, options) ->
    keyEncoding = @keyEncoding()
    options = extend {}, @_options, options
    vOptions = extend {}, options
    start = encodeKey(path, start, keyEncoding, options) if start isnt undefined
    vOptions.path = options.path
    end = encodeKey(path, end, keyEncoding, options) if end isnt undefined
    super(start, end)
  #TODO: approximateSizeAsync should not be here.
  approximateSizeAsync:(path, start, end, callback) ->
    keyEncoding = @keyEncoding()
    options = extend {}, @_options, options
    vOptions = extend {}, options
    start = encodeKey(path, start, keyEncoding, options) if start isnt undefined
    vOptions.path = options.path
    end = encodeKey(path, end, keyEncoding, options) if end isnt undefined
    super(start, end, callback)
  iterator: (options) ->
    options = extend {}, @_options, options
    vOptions = extend {}, options
    vPath = options.path || []
    valueEncoding = @valueEncoding options
    #the key is lowerBound or upperBound.
    #if opts.start is exists then lowBound key is opt.start
    encodeKeyPath = (key) ->
      vOptions.path = options.path
      encodeKey(vPath, key, null, vOptions)

    #convert the lower/upper bounds to real lower/upper bounds.
    #codec.lowerBound, codec.upperBound are default bounds in case of the options have no bounds.
    toLtgt(options, options, encodeKeyPath, codec.lowerBound, codec.upperBound) if options.bounded isnt false
    options.keyAsBuffer = options.valueAsBuffer = false
    #console.log("it:opts:", options)
    super(options)
  createDecoder: (options) ->
    options = extend {}, @_options, options
    valueEncoding = @valueEncoding options
    self = this
    if options.keys isnt false && options.values isnt false
      return (key, value) ->
        result =
          key: self.decodeKey(key, options)
          value: if valueEncoding then valueEncoding.decode(value, options) else value
        if options.absoluteKey isnt true
          result.path = toPath(options.path)
        result
    if options.values isnt false
      return (_, value) -> if valueEncoding then valueEncoding.decode(value, options) else value
    if options.keys isnt false
      return (key) -> self.decodeKey(key, options)
    return ->
  _addHookTo: (hooks, opType, range, path, callback)->
    path = resolvePathArray @_options.path, path if @_options.path
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

  subkey: (aKeyPath, aOptions, aReadyCallback)->
    @Subkey(aKeyPath, aOptions, aReadyCallback)

  root: (aOptions, aReadyCallback)->
    @Subkey(null, aOptions, aReadyCallback)

