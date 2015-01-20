isFunction    = require("abstract-object/lib/util/isFunction")
isString      = require("abstract-object/lib/util/isString")
isObject      = require("abstract-object/lib/util/isObject")
isArray       = require("abstract-object/lib/util/isArray")
inherits      = require("abstract-object/lib/util/inherits")
RefObject     = require("abstract-object/RefObject")
WriteStream   = require("nosql-stream/lib/write-stream")
ReadStream    = require('nosql-stream/lib/read-stream')
streamConsts  = require('nosql-stream/lib/consts')
codec         = require("./codec")
path          = require("./path")
errors        = require("./errors")

ReadError     = errors.ReadError
NotFoundError = errors.NotFoundError
NotImplementedError= errors.NotImplementedError
NotOpenedError= errors.NotOpenedError
LoadingError  = errors.LoadingError

setImmediate  = global.setImmediate or process.nextTick

deprecate = require("depd")("level-subkey")
deprecate.assignProperty = (object, deprecatedProp, currentProp) ->
  if object[deprecatedProp]
    this deprecatedProp + " property, use `" + currentProp + "` instead."
    object[currentProp] = object[deprecatedProp]  unless object[currentProp]
    delete object[deprecatedProp]

assignDeprecatedPrefixOption = (options) ->
  deprecate.assignProperty options, "prefix", "path"


FILTER_INCLUDED = streamConsts.FILTER_INCLUDED
FILTER_EXCLUDED = streamConsts.FILTER_EXCLUDED
FILTER_STOPPED  = streamConsts.FILTER_STOPPED
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

module.exports = (aDbCore, aCreateReadStream = ReadStream, aCreateWriteStream = WriteStream) ->


  class Subkey
    inherits(Subkey, RefObject)
    @::__defineGetter__ "sublevels", ->
      deprecate "sublevels, all subkeys(sublevels) have cached on aDbCore now."
      r = aDbCore.cache.subkeys(toPath(@_pathArray, "*"))
      result = {}
      for k of r
        result[path.basename(k)] = r[k]
      result
    @::__defineGetter__ "name", ->
      l = @_pathArray.length
      if l > 0 then @_pathArray[l-1] else PATH_SEP
    @::__defineGetter__ "fullName", ->
      PATH_SEP + @_pathArray.join(PATH_SEP)
    @::__defineGetter__ "loadingState", ->
      vState = @_loadingState_
      if not vState? then "unload" else ["loading", "loaded", "dirtied", "modifying", "modified", "deleted"][vState]
    FILTER_INCLUDED: FILTER_INCLUDED
    FILTER_EXCLUDED: FILTER_EXCLUDED
    FILTER_STOPPED: FILTER_STOPPED
    LOADING_STATES: LOADING_STATES
    Class: Subkey
    db: aDbCore
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
        aCallback(err, result) if aCallback
      else
        setImmediate aCallback
    loadAsync: (aReadyCallback)->
      if @isUnload() and aDbCore.isOpen() is true
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
        err = if aDbCore.isOpen() then new LoadingError('this is already loaded or loading...') else new NotOpenedError()
        @dispatchError err, callback
    loadSync: -> if @_loadSync then @_loadSync() else true
    load: (aReadyCallback)-> if aReadyCallback then loadAsync(aReadyCallback) else loadSync()
    init: (aKeyPath, aOptions, aReadyCallback)->
      super()
      #codec.applyEncoding(aOptions)
      @_options = aOptions
      aKeyPath = getPathArray(aKeyPath)
      aKeyPath = if aKeyPath then path.normalizeArray(aKeyPath) else []
      @_pathArray = aKeyPath
      @self = @
      @unhooks = []
      @listeners =
        ready: @emit.bind(@, "ready")
        closing: @emit.bind(@, "closing")
        closed: @emit.bind(@, "closed")
        error: @emit.bind(@, "error")
      for event, listener of @listeners
        aDbCore.on event, listener 
      @setLoadingState "unload"
      @load(aReadyCallback)
      that = @
      @on "ready", ->
        that.load(aReadyCallback)
    final: ->
      @freeSubkeys()
      #deregister all hooks
      unhooks = @unhooks
      i = 0

      while i < unhooks.length
        unhooks[i]()
        i++
      @unhooks = []
      for event, listener of @listeners
        aDbCore.removeListener event, listener
    constructor: (aKeyPath, aOptions, aCallback)->
      if isFunction aOptions
        aCallback = aOptions
        aOptions = {}
      if not (this instanceof Subkey)
        vKeyPath = path.normalizeArray getPathArray aKeyPath
        vSubkey = aDbCore.cache.createSubkey(vKeyPath, Subkey.bind(null, vKeyPath), aOptions, aCallback)
        return vSubkey

      super(aKeyPath, aOptions, aCallback)
    parent: ()->
      p = path.dirname @path()
      subkeyCache = aDbCore.cache
      result = subkeyCache.get(p)
      #get latest parent
      while not result? and p != PATH_SEP
        p = path.dirname p
        result = subkeyCache.get(p)
      return result
    setPath: (aPath, aCallback) ->
      aPath = getPathArray(aPath)
      if aPath
        aPath = path.normalizeArray(aPath)
        vPath = @path() if @_pathArray?
        if vPath? and vPath isnt resolvePath(aPath)
          aDbCore.cache.del(vPath)
          @final()
          #@_pathArray = aPath
          @init(aPath, @_options, aCallback)
          return true
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
      aDbCore.isOpen()
    pathAsArray: ->
      @_pathArray.slice()
    prefix: deprecate["function"](->
        @pathAsArray()
      , "prefix(), use `pathAsArray()` instead, or use path() to return string path..")
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
      return Subkey(vKeyPath, @mergeOpts(opts), cb)
    sublevel: deprecate["function"]((name, opts, cb) ->
        @subkey name, opts, cb
      , "sublevel(), use `subkey(name)` or `path(name)` instead.")
 
    freeSubkeys: (aKeyPattern) ->
      unless aKeyPattern
        aKeyPattern = toPath @_pathArray, "*"
      else
        aKeyPattern = resolvePath(@_pathArray, aKeyPattern)
      vSubkeys = aDbCore.cache.subkeys(aKeyPattern)
      for k of vSubkeys
        vSubkeys[k].free()
      return
    _doOperation: (aOperation, opts, cb) ->
      if isFunction opts
        cb = opts
        opts = {}
      else opts = {}  if opts is `undefined`
      assignDeprecatedPrefixOption opts
      vPath = if isString(opts.path) and opts.path.length then getPathArray(opts.path) else @_pathArray
      that = @
      if isArray(aOperation)
        vType = "batch"
        aOperation = aOperation.map((op) ->
          separator: op.separator
          key: op.key
          value: op.value
          path: resolvePathArray vPath, op.path
          keyEncoding: op.keyEncoding # *
          valueEncoding: op.valueEncoding # * (TODO: encodings on sublevel)
          type: op.type
        )
        vInfo = [vType, aOperation]
      else
        vType = aOperation.type
        vInfo = [vType, aOperation.key, aOperation.value]
        aOperation = [
          separator: opts.separator
          path: vPath
          key: aOperation.key
          value: aOperation.value
          type: aOperation.type
        ]
      if cb
        aDbCore.batchAsync aOperation, @mergeOpts(opts), (err) ->
          unless err
            that.emit.apply that, vInfo
            cb.call that, null
          cb.call that, err  if err
      else
        aDbCore.batchSync aOperation, @mergeOpts(opts)
    ###
      put it self:
        put(cb)
        put(value, cb)
    ###
    putSync: (key, value, opts) ->
      if arguments.length is 0
        cb = key
        key = "."
        value = @value
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      aDbCore.putSync key, opts
    putAsync: (key, value, opts, callback) ->
      if arguments.length is 0 or isFunction(key)
        cb = key
        key = "."
        value = @value
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      aDbCore.putAsync key, opts, cb
    put: (key, value, opts, cb) ->
      if isFunction(key) or arguments.length is 0
        cb = key
        key = "."
        value = @value
      else if isFunction value
        cb = value
        value = key
        key = "."
      @_doOperation({key:key, value:value, type: "put"}, opts, cb)
    ###TODO: del itself would destroy itself?  see: the post hook itself in init method.
      del itself:
      del(cb)
    ###
    delSync: (key, opts) ->
      if arguments.length is 0
        key = @path() #use absolute key path to delete alias key itself
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      aDbCore.delSync key, opts
    delAsync: (key, opts, cb) ->
      if arguments.length is 0 or isFunction(key)
        cb = key
        key = @path() #use absolute key path to delete alias key itself
        opts = {}
      else if isFunction opts
        cb = opts
        opts = {}
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      aDbCore.delAsync key, opts, cb
    del: (key, opts, cb) ->
      if isFunction(key) or arguments.length is 0
        cb = key
        key = @path() #use absolute key path to delete alias key itself
      @_doOperation({key:key, type: "del"}, opts, cb)
    batchSync: (ops, opts) ->
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      aDbCore.batchSync ops, opts
    batchAsync: (ops, opts, callback) ->
      if isFunction opts
        callback = opts
        opts = {}
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      aDbCore.batchAsync ops, opts, callback
    batch: (ops, opts, cb) ->
      @_doOperation(ops, opts, cb)
    getSync: (key, opts) ->
      if isObject key
        opts = key
        key = "."
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      aDbCore.getSync key, opts
    getAsync: (key, opts, cb)->
      if isFunction opts
        cb = opts
        opts = {}
      if isObject key
        opts = key
        key = "."
      else if isFunction key
        cb = key
        opts = {}
        key = "."
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      opts.path = getPathArray opts.path, @_pathArray
      that = @
      aDbCore.getAsync key, opts, (err, value) ->
        return that.dispatchError(err, cb) if err
        cb.call that, null, value if cb
    get: (key, opts, cb) ->
      if isFunction opts
        cb = opts
        opts = {}
      if isObject key
        opts = key
        key = "."
      else if isFunction key
        cb = key
        opts = {}
        key = "."
      if cb then @getAsync(key, opts, cb) else @getSync(key, opts)
    pre: (opType, key, hook) ->
      @_addHook(key, hook, aDbCore.pre.bind(aDbCore, opType))

    post: (opType, key, hook) ->
      @_addHook(key, hook, aDbCore.post.bind(aDbCore, opType))

    readStream: (opts) ->
      opts = @mergeOpts(opts)
      assignDeprecatedPrefixOption opts
      
      #the opts.path could be relative
      opts.path = getPathArray(opts.path, @_pathArray) or @_pathArray
      stream = aCreateReadStream(aDbCore, opts)
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
      opts = @mergeOpts(opts)
      new aCreateWriteStream(@, opts)
    createWriteStream: @::writeStream

    pathStream: (opts) ->
      opts = opts or {}
      opts.separator = PATH_SEP
      opts.separatorRaw = true
      opts.gte = "0"
      @readStream opts
    createPathStream: @::pathStream

  Subkey

