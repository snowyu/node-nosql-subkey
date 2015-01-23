Codec             = require('buffer-codec')
SEP               = require('./separator')
path              = require('../path')
consts            = require('../consts')

isString          = require("abstract-object/lib/util/isString")
isFunction        = require("abstract-object/lib/util/isFunction")
register          = Codec.register
_escapeString     = Codec.escapeString
toPath            = path.join
toPathArray       = path.toArray
relativePath      = path.relative
resolvePathArray  = path.resolveArray

SUBKEY_SEPS = SEP.SUBKEY_SEPS
UNSAFE_CHARS =  SEP.UNSAFE_CHARS
PATH_SEP = SUBKEY_SEPS[0][0]
SUBKEY_SEP = SUBKEY_SEPS[1][0]

module.exports = class SubkeyCodec
  #register SubkeyCodec
  GET_OP  = consts.GET_OP
  PUT_OP  = consts.PUT_OP
  DEL_OP  = consts.DEL_OP
  TRANS_OP= consts.TRANS_OP
  HALT_OP = consts.HALT_OP
  SKIP_OP = consts.SKIP_OP

  @__defineGetter__ "PATH_SEP", ->
    PATH_SEP
  @__defineGetter__ "SUBKEY_SEP", ->
    SUBKEY_SEP
  @__defineGetter__ "SUBKEY_SEPS", ->
    SUBKEY_SEPS
  @__defineSetter__ "SUBKEY_SEPS", (value) ->
    SEP.SUBKEY_SEPS = value
    SUBKEY_SEPS = SEP.SUBKEY_SEPS
    UNSAFE_CHARS =  SEP.UNSAFE_CHARS
    PATH_SEP = SUBKEY_SEPS[0][0]
    SUBKEY_SEP = SUBKEY_SEPS[1][0]

  # apply parent's encodings to a op
  addEncodings = (op, aParent) ->
    if aParent && aParent._options
      op.keyEncoding ||= aParent._options.keyEncoding
      op.valueEncoding ||= aParent._options.valueEncoding
    op

  @lowerBound: '\u0000'
  @upperBound: '\udbff\udfff' #<U+10FFFF>
    
  @escapeString: escapeString = (aString, aUnSafeChars) ->
    aUnSafeChars = UNSAFE_CHARS unless aUnSafeChars?
    _escapeString aString, aUnSafeChars
  @unescapeString = unescapeString = decodeURIComponent


  @getPathArray: getPathArray = (aPath, aRootPath) ->
    if not aPath?
      return aPath unless aRootPath?
      return aRootPath.pathAsArray() if isFunction(aRootPath.pathAsArray)
      return aRootPath
    #is a subkey object?
    return aPath.pathAsArray() if isFunction(aPath.pathAsArray)
    if isString(aPath)
      if aRootPath
        aRootPath = aRootPath.pathAsArray() if isFunction(aRootPath.pathAsArray)
        aPath = resolvePathArray(aRootPath, aPath)
        aPath.shift(0,1)
      else aPath = toPathArray(aPath)
    #is a path array:
    aPath

  @prepareKeyPath: prepareKeyPath = (aPathArray, aKey, op) ->
    if isString(aKey) && aKey.length
      aPathArray = resolvePathArray(aPathArray, aKey)
      aPathArray.shift(0,1)
      aKey = aPathArray.pop()
      if op.separator && op.separator != PATH_SEP
        aKey = op.separator + aKey if aKey[0] != op.separator
        op.separator = undefined if aKey[0] is op.separator
    op._keyPath = [aPathArray, aKey]
    op.path = aPathArray
    op.key = aKey
    return

  @prepareOperation: prepareOperation = (preHooks, operationType, aOperation, aPathArray, aKey)->
    if aOperation.path
      addEncodings(aOperation, aOperation.path) #if aOperation.path is a subkey object.
    aPathArray = aOperation.path unless aPathArray
    aKey = aOperation.key unless aKey
    prepareKeyPath(aPathArray, aKey, aOperation)
    delete aOperation.separator unless aOperation.separator
    if preHooks and operationType and aOperation.triggerBefore isnt false
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
          triggerArgs = [operationType, aOperation, addOp]
      if triggerArgs
        result = preHooks.trigger operationType, triggerArgs
    return result

  @resolveKeyPath: resolveKeyPath = (aPathArray, aKey, op)->
    op = {} unless op
    prepareKeyPath(aPathArray, aKey, op)
    [op.path, op.key]

  @encode = encode = (aPath, aKey, aSeperator, dontEscapeSeperator)->
    keyIsStr = isString(aKey) and aKey.length > 0
    hasSep   = !!aSeperator
    aSeperator = SUBKEY_SEP unless aSeperator
    if keyIsStr
      if hasSep && aKey[0] == aSeperator then aKey = aKey.substring(1)
      aKey = escapeString(aKey)
    if dontEscapeSeperator is true
      aSeperator = PATH_SEP + aSeperator if hasSep and aSeperator isnt PATH_SEP
      hasSep = false
    if hasSep
      i = SUBKEY_SEPS[0].indexOf(aSeperator)
      if i >= 0
        aSeperator = SUBKEY_SEPS[1][i]
        if aSeperator isnt PATH_SEP then aSeperator = PATH_SEP + SUBKEY_SEPS[1][i]
      else
        aSeperator = PATH_SEP + aSeperator
    else if keyIsStr
      #try to find the separator on the key
      i = SUBKEY_SEPS[0].indexOf(aKey[0], 1)
      if i > 0
          vSeperator = PATH_SEP + SUBKEY_SEPS[1][i]
          aKey = aKey.substring(1)
    #console.log("codec.encode:",path.join(e[0]) + vSeperator + key)
    #TODO: I should encode with path.join(e[0], vSeperator + key)) simply in V8.
    #      all separators are same now.
    if aPath.length
      aPath = path.join(aPath)
    else if aSeperator.length >=2 && aSeperator[0] == PATH_SEP
      aPath = ""
    else
      aPath = PATH_SEP
    aPath + aSeperator + aKey

  indexOfType = (s) ->
    i = s.length-1
    while i>0
      c = s[i]
      if (SUBKEY_SEPS[1].indexOf(c) >=0) then return i
      --i
    return -1
  #return [path, key, separator, realSeparator]
  #the realSeparator is optional, only (aSeparator && aSeparator !== seperator
  @decode = _decode =  (s, aSeparator) ->
    i = indexOfType(s)
    if i >= 0
      vSep = s[i]
      if vSep == SUBKEY_SEP
        vSep = PATH_SEP
      else
        j = SUBKEY_SEPS[1].indexOf(vSep)
        vSep = PATH_SEP + SUBKEY_SEPS[0][j]

      vKey = unescapeString(s.substring(i+1))
      result = [s.substring(1, i).split(PATH_SEP).filter(Boolean).map(unescapeString), vKey, vSep]
      result.push(s[i]) if (isString(aSeparator) && aSeparator isnt s[i])
    result

  @_encodeKey: _encodeKey = (aPathArray, aKey, keyEncoding, options)->
    aKey = keyEncoding.encode(aKey) if keyEncoding
    if options
      vSep    = options.separator
      vSepRaw = options.separatorRaw
    encode aPathArray, aKey, vSep, vSepRaw

  @encodeKey: encodeKey = (aPathArray, aKey, keyEncoding, options, operationType, preHooks)->
    prepareOperation(preHooks, operationType, options, aPathArray, aKey)
    aPathArray = options.path
    aKey = options.key
    _encodeKey aPathArray, aKey, keyEncoding, options

  @decodeKey: decodeKey = (key, keyEncoding, options)->
    #v=[parent, key, separator, realSeparator]
    #realSeparator is optional only opts.separator && opts.separator != realSeparator
    v = _decode(key, options && options.separator)
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

  @encodeValue: (value, options)->
    if encoding = Codec(options.valueEncoding)
      encoding.encode value
    else
      value

  @decodeValue: (value, options)->
    if encoding = Codec(options.valueEncoding)
      encoding.decode value
    else
      value

  @applyEncoding: (options)->
    if options
      #options.keyEncoding = Codec(options.keyEncoding)
      #options.keyEncoding = false if options.keyEncoding and options.keyEncoding.name is 'Text'
      options.valueEncoding = Codec(options.valueEncoding)
      options.valueEncoding = false if options.valueEncoding and options.valueEncoding.name is 'Text'
      #options.encoding = Codec(options.encoding)
      #options.encoding = false if options.encoding and options.encoding.name is 'Text'
      #console.log 'keye=', options.keyEncoding.name if options.keyEncoding

  #the e is array[path, key, seperator, DontEscapeSep]
  #the seperator, DontEscapeSep is optional
  #DontEscapeSep: means do not escape the separator.
  #NOTE: if the separator is PATH_SEP then it DO NOT BE CONVERT TO SUBKEY_SEP.
  @_encode = _encode = (e)->
    sep = e[2] if e.length > 2
    dontEscapeSep = e[3] if e.length > 3
    encode e[0], e[1], sep, dontEscapeSep
    ###
    vSeperator = SUBKEY_SEP
    #e[2]: seperator
    hasSep = e.length >= 3 && e[2]
    #e[3]: DontEscapeSep
    if e.length >=4 && e[3] is true
        hasSep = false
        if e[2] then vSeperator = e[2]
        if vSeperator isnt PATH_SEP then vSeperator = PATH_SEP + vSeperator
    key = e[1]
    isStrKey = isString(key) && key.length isnt 0
    if hasSep
      vSeperator = e[2]
      i = SUBKEY_SEPS[0].indexOf(vSeperator)
      if i >= 0
        vSeperator = SUBKEY_SEPS[1][i]
        if vSeperator isnt PATH_SEP then vSeperator = PATH_SEP + SUBKEY_SEPS[1][i]
      else
        vSeperator = PATH_SEP + vSeperator
    else if isStrKey
      #try to find the separator on the key
      i = SUBKEY_SEPS[0].indexOf(key[0], 1)
      if i > 0
          vSeperator = PATH_SEP + SUBKEY_SEPS[1][i]
          key = key.substring(1)
    if isStrKey
      if hasSep && key[0] == e[2] then key = key.substring(1)
      key = escapeString(key)
    #console.log("codec.encode:",path.join(e[0]) + vSeperator + key)
    #TODO: I should encode with path.join(e[0], vSeperator + key)) simply in V8.
    #      all separators are same now.
    vPath = PATH_SEP
    if e[0].length
      vPath = path.join(e[0])
    else if vSeperator.length >=2 && vSeperator[0] == PATH_SEP
      vPath = ""
    vPath + vSeperator + key
    ###

