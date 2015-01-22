SecondaryCache        = require("secondary-cache")
minimatch             = require('minimatch')
inherits              = require("abstract-object/lib/util/inherits")
setImmediate          = setImmediate || process.nextTick

module.exports = class SubkeyCache
  inherits SubkeyCache, SecondaryCache

  constructor: -> super
  createSubkey: (keyPath, Subkey, options, callback) ->
    if options && options.forceCreate == true
      result = new Subkey(options, callback)
    else
      result = @get keyPath
      if result
        setImmediate callback.bind(result, null, result) if callback
      else
        result = new Subkey(options, callback)
        @set keyPath, result, options
        result.on "destroyed", (item) =>
          @del keyPath
      result.addRef() if !options || options.addRef != false
    result
  subkeys: (aKeyPattern)->
    result = {}
    if aKeyPattern
      @forEach (v,k)-> result[k] = v if minimatch(k, aKeyPattern)
    else
      @forEach (v,k)-> result[k] = v
    result

