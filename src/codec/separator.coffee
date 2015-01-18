isString  = require("abstract-object/lib/util/isString")
isArray   = require("abstract-object/lib/util/isArray")

#special key seperators
SUBKEY_SEPS = ["/.!", "#*&"]
UNSAFE_CHARS = SUBKEY_SEPS[0] + SUBKEY_SEPS[1] + "%"
PATH_SEP = SUBKEY_SEPS[0][0]
SUBKEY_SEP = SUBKEY_SEPS[1][0]

exports.__defineGetter__ "PATH_SEP", ->
  PATH_SEP

exports.__defineGetter__ "SUBKEY_SEP", ->
  SUBKEY_SEP

exports.__defineGetter__ "SUBKEY_SEPS", ->
  SUBKEY_SEPS

exports.__defineGetter__ "UNSAFE_CHARS", ->
  UNSAFE_CHARS

exports.__defineSetter__ "SUBKEY_SEPS", (value) ->
  if isArray(value) and value.length >= 2 and isString(value[0]) and isString(value[1]) and value[0].length > 0 and value[0].length is value[1].length
    SUBKEY_SEPS = value
    PATH_SEP = SUBKEY_SEPS[0][0]
    SUBKEY_SEP = SUBKEY_SEPS[1][0]
    UNSAFE_CHARS = SUBKEY_SEPS[0] + SUBKEY_SEPS[1] + "%"

