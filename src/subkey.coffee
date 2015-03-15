defineProperty  = require("util-ex/lib/defineProperty")
isFunction      = require("util-ex/lib/is/type/function")
isString        = require("util-ex/lib/is/type/string")
isObject        = require("util-ex/lib/is/type/object")
isArray         = require("util-ex/lib/is/type/array")
inherits        = require("inherits-ex/lib/inherits")
isInheritedFrom = require("inherits-ex/lib/isInheritedFrom")
RefObject       = require("ref-object/eventable-ref-object")
try
  WriteStream   = require("nosql-stream/lib/write-stream")
  ReadStream    = require('nosql-stream/lib/read-stream')
codec           = require("./codec")
path            = require("./path")
errors          = require("./errors")

normalizePath       = path.normalize
normalizePathArray  = path.normalizeArray
toPath              = path.join
ReadError           = errors.ReadError
NotFoundError       = errors.NotFoundError
InvalidArgumentError= errors.InvalidArgumentError
NotImplementedError = errors.NotImplementedError
NotOpenedError      = errors.NotOpenedError
LoadingError        = errors.LoadingError
setImmediate        = global.setImmediate or process.nextTick

deprecate = require("depd")("nosql-subkey")
deprecate.assignProperty = (object, deprecatedProp, currentProp) ->
  if object[deprecatedProp]
    this deprecatedProp + " property, use `" + currentProp + "` instead."
    object[currentProp] = object[deprecatedProp]  unless object[currentProp]
    delete object[deprecatedProp]

assignDeprecatedPrefixOption = (options) ->
  deprecate.assignProperty options, "prefix", "path"


PATH_SEP        = codec.PATH_SEP
SUBKEY_SEP      = codec.SUBKEY_SEP
getPathArray    = codec.getPathArray
toPath          = path.join
resolvePathArray= path.resolveArray
resolvePath     = path.resolve

version = require("../package.json").version

OBJECT_STATES = RefObject::OBJECT_STATES

# the object loading state constants:
LOADING_STATES =
  unload    : null
  loading   : 0
  loaded    : 1
  dirtied   : 2
  modifying : 3
  modified  : 4
  deleted   : 5

argumentsAre = (args, value, startIndex=0, endIndex) ->
  return true unless args and args.length
  endIndex = if endIndex? then Math.min(endIndex, args.length-1) else args.length-1
  while startIndex <= endIndex
    return false if args[startIndex] isnt value
    ++startIndex
  true

module.exports = (dbCore, DefaultReadStream = ReadStream, DefaultWriteStream = WriteStream) ->

  cache = dbCore.cache
  class Subkey
    inherits(Subkey, RefObject)

    constructor: (aKeyPath, aOptions, aCallback)->
      if isFunction aOptions
        aCallback = aOptions
        aOptions = {}
      if not (this instanceof Subkey)
        vKeyPath = if aKeyPath then normalizePathArray getPathArray aKeyPath else []
        vSubkey = cache.createSubkey(toPath(vKeyPath), Subkey.bind(null, vKeyPath), aOptions, aCallback)
        return vSubkey
      super(aKeyPath, aOptions, aCallback)
    initialize: (aKeyPath, aOptions, aReadyCallback)->
      super()
      #codec.applyEncoding(aOptions)
      #@db = dbCore
      defineProperty @, 'db', dbCore
      #@_options = aOptions
      defineProperty @, '_options', aOptions
      aKeyPath = getPathArray(aKeyPath)
      aKeyPath = if aKeyPath then normalizePathArray(aKeyPath) else []
      #@_pathArray = aKeyPath
      defineProperty @, '_pathArray', aKeyPath
      #@self = @
      defineProperty @, 'self', @, writable: false
      #@unhooks = []
      defineProperty @, 'unhooks', []
      #@listeners =
      defineProperty @, 'listeners',
        ready: @emit.bind(@, "ready")
        closing: @emit.bind(@, "closing")
        closed: @emit.bind(@, "closed")
        error: @emit.bind(@, "error")
      for event, listener of @listeners
        dbCore.on event, listener
      @_initialize(aKeyPath, aOptions) if @_initialize
      defineProperty @, '_loadingState_', null
      @setLoadingState "unload"
      @load(aReadyCallback)
      that = @
      @on "ready", ->
        that.load(aReadyCallback)
    finalize: (isFreeSubkeys)->
      @_finalize(isFreeSubkeys) if @_finalize
      @freeSubkeys() if isFreeSubkeys isnt false
      #deregister all hooks
      unhooks = @unhooks
      i = 0

      while i < unhooks.length
        unhooks[i]()
        i++
      @unhooks = []
      for event, listener of @listeners
        dbCore.removeListener event, listener

    defineProperty @::, "sublevels", undefined,
      get: ->
        deprecate "sublevels, all subkeys(sublevels) have cached on dbCore.cache now."
        r = cache.subkeys(toPath(@_pathArray, "*"))
        result = {}
        for k of r
          result[path.basename(k)] = r[k]
        result
    defineProperty @::, "name", undefined,
      get: ->
        l = @_pathArray.length
        if l > 0 then @_pathArray[l-1] else PATH_SEP
    defineProperty @::, "fullName", undefined,
      get: ->
        PATH_SEP + @_pathArray.join(PATH_SEP)
    defineProperty @::, "loadingState", undefined,
      get: ->
        vState = @_loadingState_
        if not vState? then "unload" else ["loading", "loaded", "dirtied", "modifying", "modified", "deleted"][vState]
    LOADING_STATES: LOADING_STATES
    Class: Subkey
    version: version
    setLoadingState: (value, emitted = false, param1, param2)->
      @_loadingState_ = LOADING_STATES[value]
      @emit value, @, param1, param2 if emitted
    isLoading: ->
      @_loadingState_ is LOADING_STATES.loading
    isLoaded: ->
      @_loadingState_ >= LOADING_STATES.loaded
    isUnload: ->
      not @_loadingState_?
    _loadAsync: (aCallback) ->
      if @_loadSync then setImmediate =>
        try
          result = @loadSync()
        catch e
          err = e
          @dispatchError err, aCallback
          return
        aCallback(null, result) if aCallback
      else
        setImmediate aCallback.bind(@, null, @)
    loadAsync: (aReadyCallback)->
      if @isUnload() and dbCore.isOpen() is true
        @setLoadingState "loading"
        @_loadAsync (err, result)=>
          if not err
            @setLoadingState "loaded"
            if aReadyCallback
              aReadyCallback err, result
          else
            @setLoadingState "unload"
            @dispatchError err, callback
      else
        err = if dbCore.isOpen() then new LoadingError('this is already loaded or loading...') else new NotOpenedError()
        @dispatchError err, callback
    loadSync: ->
      result = @isUnload() and dbCore.isOpen() is true
      if result 
        @setLoadingState "loading"
        result = if @_loadSync then @_loadSync() else true
        @setLoadingState "loaded"
      else
        err = if dbCore.isOpen() then new LoadingError('this is already loaded or loading...') else new NotOpenedError()
        throw err
      result
    load: (aReadyCallback)-> if aReadyCallback then @loadAsync(aReadyCallback) else @loadSync()
    parent: (options, callback)->
      return undefined unless @_pathArray.length
      if isFunction options
        callback = options
        options = {}
      if options
        createIfMissing = options.createIfMissing
        latestParent    = options.latestParent
        delete options.createIfMissing
        delete options.latestParent
      options = @mergeOpts options
      vkeyPath = @_pathArray.slice(0, @_pathArray.length-1)
      p = toPath(vkeyPath)
      if createIfMissing is true
        cache.createSubkey(p, Subkey.bind(null, vkeyPath), options, callback)
      else
        #get latest parent
        result = cache.get p
        throw new NotFoundError(p+" path can not be found in cache") if not result? and latestParent isnt true
        while not result? and p != PATH_SEP
          p = path.dirname p
          result = cache.get(p)
        callback(null, result) if isFunction callback
        result
    # setPath will remove itself from cache if successful.
    setPath: (aPath, aCallback) ->
      aPath = getPathArray(aPath)
      if aPath
        aPath = normalizePathArray(aPath)
        vPath = @fullName if @_pathArray?
        if vPath? and vPath isnt resolvePath(aPath)
          cache.del(vPath)
          @finalize()
          #@_pathArray = aPath
          @initialize(aPath, @_options, aCallback)
          return true
      aCallback new InvalidArgumentError('path argument is invalid') if aCallback
      false
    _addHook: (key, callback, hooksAdd) ->
      unhook = hooksAdd key, @_pathArray, callback
      @unhooks.push unhook
      lst = @unhooks
      return ->
        i = lst.indexOf(unhook)
        lst.splice i, 1  if ~i
        unhook()
    mergeOpts: (opts) ->
      o = {}
      if @_options
        for k of @_options
          o[k] = @_options[k]  if @_options[k] isnt `undefined`
      if opts
        for k of opts
          o[k] = opts[k]  if opts[k] isnt `undefined`
      o
    #the writeStream use db.isOpen and db.once('ready') to ready write stream.
    isOpen: ->
      dbCore.isOpen()
    pathAsArray: ->
      @_pathArray.slice()
    prefix: deprecate["function"] (-> @pathAsArray())
      , "prefix(), use `pathAsArray()` instead, or use path() to return string path.."

    path: (aPath, aOptions, aCallback) ->
      if aPath is `undefined`
        @fullName
      else
        @subkey aPath, aOptions, aCallback

    subkey: (name, opts, cb) ->
      vKeyPath = resolvePathArray(@_pathArray, name)
      vKeyPath.shift 0, 1
      if isFunction opts
        cb = opts
        opts = {}
      opts = @mergeOpts(opts)
      opts.addRef = false
      return Subkey(vKeyPath, opts, cb)
    createSubkey: (name, opts, cb) ->
      vKeyPath = resolvePathArray(@_pathArray, name)
      vKeyPath.shift 0, 1
      if isFunction opts
        cb = opts
        opts = {}
      return Subkey(vKeyPath, @mergeOpts(opts), cb)
    createPath: @::createSubkey
    sublevel: deprecate["function"] ((name, opts, cb) ->@subkey name, opts, cb)
      , "sublevel(), use `subkey(name)` or `path(name)` instead."

    freeSubkeys: (aKeyPattern) ->
      unless aKeyPattern
        aKeyPattern = toPath @_pathArray, "*"
      else
        aKeyPattern = resolvePath(@_pathArray, aKeyPattern)
      vSubkeys = cache.subkeys(aKeyPattern)
      for k of vSubkeys
        vSubkeys[k].free()
      return
    putSync: (key, value, opts) ->
      if argumentsAre(arguments, undefined, 1)
        value = key
        key = "."
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      dbCore.putSync key, value, opts
    ###
      put it self:
        put(value, cb)
    ###
    putAsync: (key, value, opts, callback) ->
      if argumentsAre(arguments, undefined, 1) or isFunction(value)
        callback = value
        value = key
        key = "."
      if isFunction opts
        callback = opts
        opts = undefined
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      dbCore.putAsync key, value, opts, callback
    put: (key, value, opts, cb) ->
      if isFunction value
        cb = value
        value = key
        key = "."
      else if isFunction opts
        cb = opts
        opts = undefined
      if cb then @putAsync key, value, opts, cb else @putSync key, value, opts
    ###TODO: del itself would destroy itself?  see: the post hook itself in init method.
      del itself:
      del(cb)
    ###
    delSync: (key, opts) ->
      if argumentsAre(arguments, undefined)
        key = @path() #use absolute key path to delete alias key itself
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      dbCore.delSync key, opts
    delAsync: (key, opts, cb) ->
      if argumentsAre(arguments, undefined) or isFunction(key)
        cb = key
        key = @path() #use absolute key path to delete alias key itself
        opts = undefined
      else if isFunction opts
        cb = opts
        opts = undefined
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      dbCore.delAsync key, opts, cb
    del: (key, opts, cb) ->
      if isFunction(key) or arguments.length is 0
        cb = key
        key = @path() #use absolute key path to delete alias key itself
      else if isFunction opts
        cb = opts
        opts = undefined
      if cb then @delAsync key, opts, cb else @delSync key, opts
    batchSync: (ops, opts) ->
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      dbCore.batchSync ops, opts
    batchAsync: (ops, opts, callback) ->
      if isFunction opts
        callback = opts
        opts = undefined
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      dbCore.batchAsync ops, opts, callback
    batch: (ops, opts, cb) ->
      if isFunction opts
        cb = opts
        opts = undefined
      if cb then @batchAsync ops, opts, cb else @batchSync ops, opts
    getSync: (key, opts) ->
      if argumentsAre(arguments, undefined)
        opts = key
        key = "."
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      dbCore.getSync key, opts
    getAsync: (key, opts, cb)->
      if isFunction opts
        cb = opts
        opts = undefined
      else if isFunction key
        cb = key
        opts = undefined
        key = "."
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      that = @
      dbCore.getAsync key, opts, (err, value) ->
        return that.dispatchError(err, cb) if err
        cb.call that, null, value if cb
    get: (key, opts, cb) ->
      if isFunction opts
        cb = opts
        opts = undefined
      else if isFunction key
        cb = key
        opts = undefined
        key = "."
      if cb then @getAsync(key, opts, cb) else @getSync(key, opts)
    pre: (opType, key, hook) ->
      @_addHook(key, hook, dbCore.pre.bind(dbCore, opType))

    post: (opType, key, hook) ->
      @_addHook(key, hook, dbCore.post.bind(dbCore, opType))

    prepareFindOptions: (aOptions)->
      aOptions = @mergeOpts(aOptions)
      assignDeprecatedPrefixOption aOptions
      makeData = aOptions.makeData

      if not aOptions.makeData
        aOptions.makeData = if aOptions.keys isnt false and aOptions.values isnt false then (key, value) ->
            key: key
            value: value
        else if aOptions.values is false then (key) -> key
        else if aOptions.keys   is false then (_, value) -> value
        else ->

      #the aOptions.path could be relative
      aOptions.path = getPathArray(aOptions.path, @_pathArray) or @_pathArray
      aOptions
    findSync: (aOptions)->
      aOptions = @prepareFindOptions aOptions
      makeData = aOptions.makeData
      it = dbCore.iterator(aOptions)
      result = []
      item = it.nextSync()
      while item isnt false
        result.push makeData(item.key, item.value)
        item = it.nextSync()
      result
    findAsync: (aOptions, callback)->
      if isFunction aOptions
        callback = aOptions
        aOptions = undefined
      throw new InvalidArgumentError('callback argument required.') unless callback
      aOptions = @prepareFindOptions aOptions
      makeData = aOptions.makeData
      it = dbCore.iterator(aOptions)
      result = []
      nextOne = ->
        it.next (err, key, value)->
          return callback err, result if err
          return callback err, result if !arguments.length
          result.push makeData(key, value)
          nextOne()
      nextOne()
    find: (aOptions, callback)->
      if isFunction aOptions
        callback = aOptions
        aOptions = undefined
      if callback then @findAsync aOptions, callback else @findSync aOptions

    readStream: (opts) ->
      throw new NotImplementedError("please `npm install nosql-stream` to use streamable feature") unless DefaultReadStream
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts

      #the opts.path could be relative
      opts.path = getPathArray(opts.path, @_pathArray) or @_pathArray
      stream = DefaultReadStream(dbCore, opts)
      stream
    createReadStream: @::readStream

    valueStream: (opts) ->
      opts = opts or {}
      opts.values = true
      opts.keys = false
      @readStream opts
    createValueStream: @::valueStream

    keyStream: (opts) ->
      opts = opts or {}
      opts.values = false
      opts.keys = true
      @readStream opts
    createKeyStream: @::keyStream

    writeStream: (opts) ->
      throw new NotImplementedError("please `npm install nosql-stream` to use streamable feature") unless DefaultWriteStream
      opts = @mergeOpts(opts)
      new DefaultWriteStream(@, opts)
    createWriteStream: @::writeStream

    pathStream: (opts) ->
      opts = opts or {}
      opts.separator = PATH_SEP
      opts.separatorRaw = true
      opts.gte = "0"
      @readStream opts
    createPathStream: @::pathStream

  Subkey
