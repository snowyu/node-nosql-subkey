ltgt = require("ltgt")
minimatch = require("minimatch")

isArray = Array.isArray
isBuffer = Buffer.isBuffer


#compare two array items
isArrayLike = (a) -> isArray(a) or isBuffer(a)
isPrimitive = (a) ->
  a = typeof a
  "string" is a or "number" is a
has = (o, k) -> Object.hasOwnProperty.call o, k

compare = (a, b) ->
  if isArrayLike(a) and isArrayLike(b)
    l = Math.min(a.length, b.length)
    i = 0

    while i < l
      c = compare(a[i], b[i])
      return c  if c
      i++
    return a.length - b.length
  return (if a < b then -1 else (if a > b then 1 else 0))  if isPrimitive(a) and isPrimitive(b)
  throw new Error("items not comparable:" + JSON.stringify(a) + " " + JSON.stringify(b))

#this assumes that the prefix is of the form:
# [Array, string]

# aRange   = [pathArray, key(string)]
# aKeyPath = [pathArray, key(string)]
compareKeyPath = (aRange, aKeyPath) ->
  return false  if aRange.length > aKeyPath.length
  l = aRange.length - 1
  lastRange = aRange[l]
  lastKeyPath = aKeyPath[l]
  return false  if typeof lastRange isnt typeof lastKeyPath
  return false  if "string" is typeof lastRange and minimatch(lastKeyPath, lastRange) is false
  
  #handle case where there is no key prefix
  #(a hook on an entire sublevel)
  l++  if aRange.length is 1 and isArrayLike(lastRange)
  return false  if compare(aRange[l], aKeyPath[l])  while l--
  true

#check that everything up to the last item is equal
#then check the last item starts with

#  return ltgt.contains(range, key, compare)
addPathToRange = (path, range) ->
  r = {}
  r.lt = [path, range.lt]  if has(range, "lt")
  r.gt = [path, range.gt]  if has(range, "gt")
  r.lte = [path, range.lte]  if has(range, "lte")
  r.gte = [path, range.gte]  if has(range, "gte")
  if has(range, "start")
    if range.reverse
      r.lte = [path, range.start]
    else
      r.gte = [path, range.start]
  if has(range, "end")
    if range.reverse
      r.gte = [path, range.end]
    else
      r.lte = [path, range.end]
  r.gte = [path, range.min]  if has(range, "min")
  r.lte = [path, range.max]  if has(range, "max")
  r.reverse = !!range.reverse
  
  #if there where no ranges, then then just use a path.
  return [path]  if not r.gte and not r.lte
  r

#whether the key is in range.
exports = module.exports = (range, key) ->
  return compareKeyPath(range, key)  if isArrayLike(range)
  return false  if range.lt and compare(key, range.lt) >= 0
  return false  if range.lte and compare(key, range.lte) > 0
  return false  if range.gt and compare(key, range.gt) <= 0
  return false  if range.gte and compare(key, range.gte) < 0
  true

exports.compare = compare
exports.prefix = compareKeyPath
exports.addPathToRange = addPathToRange
