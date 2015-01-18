util      = require("abstract-object/lib/util")
Codec     = require('buffer-codec')
path      = require('../path')
SEP       = require('./separator')

TextCodec = Codec['text']
register  = Codec.register
isString  = util.isString
isFunction= util.isFunction
toPath    = path.join

SUBKEY_SEPS = SEP.SUBKEY_SEPS
UNSAFE_CHARS =  SEP.UNSAFE_CHARS
PATH_SEP = SUBKEY_SEPS[0][0]
SUBKEY_SEP = SUBKEY_SEPS[1][0]

module.exports = class SubkeyCodec
  #register SubkeyCodec

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

  pathToPathArray = path.toArray

  @getPathArray = getPathArray = (aPath, aParentPath) ->
    return aPath unless aPath?
    #is a subkey object?
    return aPath.pathAsArray() if isFunction(aPath.pathAsArray)
    if isString(aPath)
      if aParentPath
        aPath = path.resolveArray(aParentPath, aPath)
        aPath.shift(0,1)
      else aPath = pathToPathArray(aPath)
    #is a path array:
    aPath

  @resolveKeyPath = resolveKeyPath = (aPathArray, aKey)->
    if isString(aKey) && aKey.length
        vPath = path.resolveArray(aPathArray, aKey)
        isAbsolutePath = vPath.shift(0,1)
        aKey = vPath.pop()
        [vPath, aKey]
    else
        [aPathArray, aKey]

  @escapeString = escapeString = (aString, aUnSafeChars) ->
    aUnSafeChars = UNSAFE_CHARS unless aUnSafeChars?
    Codec.escapeString aString, aUnSafeChars
  @unescapeString = unescapeString = decodeURIComponent

  indexOfType = (s) ->
    i = s.length-1
    while i>0
      c = s[i]
      if (SUBKEY_SEPS[1].indexOf(c) >=0) then return i
      --i
    return -1
  @encode = (aPath, aKey, aSeperator, dontEscapeSeperator)->
    keyIsStr = isString(aKey) and aKey.length isnt 0
    hasSep   = !!aSeperator
    aSeperator = SUBKEY_SEP unless aSeperator
    if keyIsStr
      if hasSep && key[0] == aSeperator then key = key.substring(1)
      key = escapeString(key)
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
      i = SUBKEY_SEPS[0].indexOf(key[0], 1)
      if i > 0
          vSeperator = PATH_SEP + SUBKEY_SEPS[1][i]
          key = key.substring(1)
    #console.log("codec.encode:",path.join(e[0]) + vSeperator + key)
    #TODO: I should encode with path.join(e[0], vSeperator + key)) simply in V8.
    #      all separators are same now.
    if aPath.length
      aPath = path.join(aPath)
    else if aSeperator.length >=2 && aSeperator[0] == PATH_SEP
      aPath = ""
    else
      aPath = PATH_SEP
    aPath + aSeperator + key

  #the e is array[path, key, seperator, DontEscapeSep]
  #the seperator, DontEscapeSep is optional
  #DontEscapeSep: means do not escape the separator.
  #NOTE: if the separator is PATH_SEP then it DO NOT BE CONVERT TO SUBKEY_SEP.
  @_encode = _encode = (e)->
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

  #return [path, key, separator, realSeparator]
  #the realSeparator is optional, only (aSeparator && aSeparator !== seperator
  @_decode = _decode =  (s, aSeparator) ->
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

  @encodeValue: (value, options)->
    if encoding = Codec(options.valueEncoding) or Codec(options.encoding)
      encoding.encode value
    else
      value
  @decodeValue: (value, options)->
    if encoding = Codec(options.valueEncoding) or Codec(options.encoding)
      encoding.decode value
    else
      value
  # options.path, options.separator
  # key=[pathArray, key]
  @encodeKey: (key, options)->
    if options
      key[1] = encoding.encode key[1] if encoding = Codec(options.keyEncoding) or Codec(options.encoding)
      vSep    = options.separator
      vSepRaw = options.separatorRaw
      key.push vSep, vSepRaw
    _encode key
    
    ###
    vPath = options.path
    vPath = if vPath then getPathArray(vPath) else []
    vPath = resolveKeyPath vPath, key
    console.log "p=", vPath
    if options
      vPath[1] = encoding.encode vPath[1] if encoding = Codec(options.keyEncoding) or Codec(options.encoding)
      vSep    = options.separator
      vSepRaw = options.separatorRaw
      vPath.push vSep, vSepRaw
    _encode vPath
    ###

  @lowerBound = '\u0000'
  @upperBound = '\udbff\udfff'
    
  @decodeKey: (key, options)->
    #v=[parent, key, separator, realSeparator]
    #realSeparator is optional only opts.separator && opts.separator != realSeparator
    v = _decode(key, options && options.separator)
    vSep = v[2] #separator
    vSep = PATH_SEP unless vSep?  #if the precodec is other codec.
    if options
      key = if encoding = Codec(options.keyEncoding) then encoding.decode(v[1], options) else v[1]
      if options.absoluteKey
          key = toPath(v[0]) + vSep + key
      else if options.path && isString(key) && key != ""
          vPath = path.relative(options.path, v[0])
          if vPath is "" and vSep is PATH_SEP
            vSep = "" 
          else if vSep.length >= 2
            vSep = vSep.substring(1)
          key = vPath + vSep + key
    else
      key = v[1]
    key
  @applyEncoding: (options)->
    if options
      #options.keyEncoding = Codec(options.keyEncoding)
      #options.keyEncoding = false if options.keyEncoding and options.keyEncoding.name is 'Text'
      options.valueEncoding = Codec(options.valueEncoding)
      options.valueEncoding = false if options.valueEncoding and options.valueEncoding.name is 'Text'
      #options.encoding = Codec(options.encoding)
      #options.encoding = false if options.encoding and options.encoding.name is 'Text'
      #console.log 'keye=', options.keyEncoding.name if options.keyEncoding

