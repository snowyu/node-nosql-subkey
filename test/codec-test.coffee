chai            = require 'chai'
sinon           = require 'sinon'
sinonChai       = require 'sinon-chai'
should          = chai.should()
expect          = chai.expect
assert          = chai.assert
Errors          = require 'abstract-object/Error'
util            = require 'abstract-object/util'
Codec           = require 'buffer-codec'
inherits        = require 'abstract-object/lib/util/inherits'
isInheritedFrom = require 'abstract-object/lib/util/isInheritedFrom'
isObject        = require 'abstract-object/lib/util/isObject'
codec           = require '../src/codec'
path            = require '../src/path'

setImmediate          = setImmediate || process.nextTick
InvalidArgumentError  = Errors.InvalidArgumentError
PATH_SEP              = codec.PATH_SEP
SUBKEY_SEP            = codec.SUBKEY_SEP
SUBKEY_SEPS           = codec.SUBKEY_SEPS
_encodeKey            = codec._encodeKey
toPath                = path.join

chai.use(sinonChai)

#compare two array items
compare = (a, b) ->
 if(Array.isArray(a) && Array.isArray(b))
    l = Math.min(a.length, b.length)
    for i in [0...l]
      c = compare(a[i], b[i])
      if c then return c
    return a.length - b.length
  if ('string' == typeof a && 'string' == typeof b)
    return a < b ? -1 : a > b ? 1 : 0

  throw new Error('items not comparable:'
    + JSON.stringify(a) + ' ' + JSON.stringify(b))

random = () -> Math.random() - 0.5

describe "SubkeyCodec", ->

  describe "encoding keyPath correctly", ->
    expected = [
      [[], 'foo'],
      [['foo'], 'bar'],
      [['foo', 'bar'], 'baz'],
      [['foo', 'bar'], 'blerg'],
      [['foobar'], 'barbaz'],
    ]

    expectedDecoded = [
      [[], 'foo', PATH_SEP],
      [['foo'], 'bar', PATH_SEP],
      [['foo', 'bar'], 'baz', PATH_SEP],
      [['foo', 'bar'], 'blerg', PATH_SEP],
      [['foobar'], 'barbaz', PATH_SEP],
    ]

    others = [
      [[], 'foo', SUBKEY_SEPS[0][1]],
      [['foo'], 'bar', SUBKEY_SEPS[0][1]],
      [['foo', 'bar'], 'baz', SUBKEY_SEPS[0][1]],
      [['foo', 'bar'], 'blerg', SUBKEY_SEPS[0][1]],
      [['foobar'], 'barbaz', SUBKEY_SEPS[0][1]],
    ]
    othersDecoded = [
      [[], 'foo', PATH_SEP + SUBKEY_SEPS[0][1]],
      [['foo'], 'bar', PATH_SEP + SUBKEY_SEPS[0][1]],
      [['foo', 'bar'], 'baz', PATH_SEP + SUBKEY_SEPS[0][1]],
      [['foo', 'bar'], 'blerg', PATH_SEP + SUBKEY_SEPS[0][1]],
      [['foobar'], 'barbaz', PATH_SEP + SUBKEY_SEPS[0][1]],
    ]
    format = codec
    encoded = expected.map(format._encode)
    
    it "ordering", ->
      expected.sort(compare)
      actual = expected.slice()
          .sort(random)
          .map(format._encode)
          .sort()
          .map(format.decode)

      assert.deepEqual(actual, expectedDecoded)
    it "ordering others", ->
      others.sort(compare)
      actual = others.slice()
          .sort(random)
          .map(format._encode)
          .sort()
          .map(format.decode)

      assert.deepEqual(actual, othersDecoded)
    it "ranges", ->
      gt = (a, b, i, j) ->
        assert.equal(a > b,  i > j,  a + ' gt '  + b + '==' + i >  j)
      gte = (a, b, i, j) ->
        assert.equal(a >= b, i >= j, a + ' gte ' + b + '==' + i >= j)
      lt =  (a, b, i, j) ->
        assert.equal(a < b,  i < j,  a + ' lt '  + b + '==' + i <  j)
      lte = (a, b, i, j) ->
        assert.equal(a <= b, i <= j, a + ' lte ' + b + '==' + i <= j)
      check = (j, cmp) ->
        item = encoded[j]
        for i in [0...expected.length]
          #first check less than.
          cmp(item, encoded[i], j, i)

      for i in [0...expected.length]
        check(i, gt)
        check(i, gte)
        check(i, lt)
        check(i, lte)
    it "SUBKEY_SEPS", ->
      oldSeps = format.SUBKEY_SEPS
      format.SUBKEY_SEPS = ["/!~", ".$-"]
      assert.deepEqual(format.SUBKEY_SEPS, ["/!~", ".$-"])
      assert.equal(format.SUBKEY_SEP, ".")
      assert.equal(format.escapeString("Hello~world!"), "Hello%7eworld%21")
      assert.equal(format._encode([["path"], "Key!ABC", "!"]), '/path/$Key%21ABC')
      assert.equal(format._encode([["path"], "KeyABC", "!"]), '/path/$KeyABC')
      assert.equal(format._encode([["path"], "!KeyABC", "!"]), '/path/$KeyABC')
      assert.equal(format._encode([["path"], "!", "!"]), '/path/$')
      assert.equal(format._encode([["path"], "", "!"]), '/path/$')
      assert.equal(format._encode([["path"], "key"]), '/path.key')
      assert.equal(format._encode([[], "\uffff", '!']), '/$\uffff')
      assert.deepEqual(format.decode('/path/Key$ABC'), [["path", "Key"], "ABC", "/!"])
      assert.deepEqual(format.decode('/path/Key.ABC'), [["path", "Key"], "ABC", "/"])
      format.SUBKEY_SEPS = oldSeps
