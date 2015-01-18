var util = require("abstract-object/lib/util")
var isString = util.isString
var isArray = util.isArray

//special key seperators
var SUBKEY_SEPS = ['/.!', '#*&']
var UNSAFE_CHARS =  SUBKEY_SEPS[0] + SUBKEY_SEPS[1] + '%'
var PATH_SEP = SUBKEY_SEPS[0][0], SUBKEY_SEP = SUBKEY_SEPS[1][0]

exports.__defineGetter__("PATH_SEP", function() {
  return PATH_SEP
})

exports.__defineGetter__("SUBKEY_SEP", function() {
  return SUBKEY_SEP
})

exports.__defineGetter__("SUBKEY_SEPS", function() {
  return SUBKEY_SEPS
})

exports.__defineGetter__("UNSAFE_CHARS", function() {
  return UNSAFE_CHARS
})

exports.__defineSetter__("SUBKEY_SEPS", function(value) {
    if (Array.isArray(value) && value.length>=2 && isString(value[0]) && isString(value[1]) && value[0].length>0 && value[0].length===value[1].length) {
      SUBKEY_SEPS  = value
      PATH_SEP     = SUBKEY_SEPS[0][0]
      SUBKEY_SEP   = SUBKEY_SEPS[1][0]
      UNSAFE_CHARS = SUBKEY_SEPS[0] + SUBKEY_SEPS[1] + '%'
    }
})


