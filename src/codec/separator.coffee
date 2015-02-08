isString  = require("abstract-object/lib/util/isString")
isArray   = require("abstract-object/lib/util/isArray")

#special key seperators
SUBKEY_SEPS = ["/.!", "#*&"]
UNSAFE_CHARS = SUBKEY_SEPS[0] + SUBKEY_SEPS[1] + "%"
PATH_SEP = SUBKEY_SEPS[0][0]
SUBKEY_SEP = SUBKEY_SEPS[1][0]

defineProperty = Object.defineProperty

exports = module.exports = {}

defineProperty exports, "PATH_SEP",
  get:->PATH_SEP

defineProperty exports, "SUBKEY_SEP",
  get:->SUBKEY_SEP

defineProperty exports, "SUBKEY_SEPS",
  get:->SUBKEY_SEPS
  set:(value)->
    if isArray(value) and value.length >= 2 and isString(value[0]) and isString(value[1]) and value[0].length > 0 and value[0].length is value[1].length
      SUBKEY_SEPS = value
      PATH_SEP = SUBKEY_SEPS[0][0]
      SUBKEY_SEP = SUBKEY_SEPS[1][0]
      UNSAFE_CHARS = SUBKEY_SEPS[0] + SUBKEY_SEPS[1] + "%"

defineProperty exports, "UNSAFE_CHARS",
  get:->UNSAFE_CHARS


