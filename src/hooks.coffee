inRange = require("./range")
#consts  = require("./consts")

module.exports = ->
  # {operationType: []}
  hooks = {}
  add: (aOperationType, aRange, hookProc) ->
    m =
      range: range
      hook: hook

    hooks[aOperationType] = [] unless hooks[aOperationType]?
    hooks[aOperationType].push m
    
    #call this to remove
    ->
      i = hooks[aOperationType].indexOf(m)
      hooks[aOperationType].splice i, 1  if ~i

  
  #remove all listeners within a range.
  #this will be used to close a sublevel.
  removeAll: (range) ->
    if range is `undefined`
      hooks = {}
    else
      throw new Error("not implemented")

  trigger: (aOperationType, key, args) ->
    i = 0
    otHooks = hooks[aOperationType]
    if otHooks then while i < otHooks.length
      test = otHooks[i]
      test.hook.apply this, args if inRange(test.range, key)
      i++
