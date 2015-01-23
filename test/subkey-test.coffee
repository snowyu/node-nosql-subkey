chai            = require 'chai'
sinon           = require 'sinon'
sinonChai       = require 'sinon-chai'
should          = chai.should()
expect          = chai.expect
assert          = chai.assert
SubkeyNoSQL     = require '../src/nosql-subkey'
#AbstractNoSQL   = require 'abstract-nosql'
Errors          = require 'abstract-object/Error'
util            = require 'abstract-object/util'
Codec           = require 'buffer-codec'
EncodingIterator= require 'encoding-iterator'
inherits        = require 'abstract-object/lib/util/inherits'
isInheritedFrom = require 'abstract-object/lib/util/isInheritedFrom'
isObject        = require 'abstract-object/lib/util/isObject'
FakeDB          = require './fake-nosql'
codec           = require '../src/codec'
path            = require '../src/path'

setImmediate          = setImmediate || process.nextTick
InvalidArgumentError  = Errors.InvalidArgumentError
PATH_SEP              = codec.PATH_SEP
SUBKEY_SEP            = codec.SUBKEY_SEP
_encodeKey            = codec._encodeKey
toPath                = path.join

chai.use(sinonChai)

FakeDB = SubkeyNoSQL(FakeDB)

genData = (db, path = "op", opts, count = 10)->
  data = for i in [1..count]
    key: myKey: Math.random()
    value: Math.random()
    path: path
  db.batch data, opts
  vParentPath = opts.path if opts
  _opts = {}
  for item in data
    key = getEncodedKey db, item.key, item, vParentPath
    _opts.valueEncoding = item.valueEncoding
    _opts.valueEncoding = opts.valueEncoding if opts and not _opts.valueEncoding
    valueEncoding = db.valueEncoding _opts
    value = if valueEncoding then valueEncoding.encode(item.value) else item.value
    db.data.should.have.property key, value
  data

getEncodedKey = (db, key, options, parentPath) ->
  options = {} unless options
  _encodeKey db.getPathArray(options, parentPath), key, db.keyEncoding(options), options
getEncodedValue = (db, value, options) ->
  encoding = db.valueEncoding options
  value = encoding.encode value if encoding
  value

getEncodedOps = (db, ops, opts) ->
  vParentPath = opts.path if opts
  ops.slice().map (op) ->
    key: getEncodedKey db, op.key, op, vParentPath
    value: if op.value then JSON.stringify op.value else op.value
    type: op.type
    path: path = db.getPathArray(op, vParentPath)
    _keyPath: [path, op.key]

describe "Subkey", ->

  before ->
    @db = new FakeDB()
    @db.open({keyEncoding:'json', valueEncoding: 'json'})
    @root = @db.root()
  after ->
    @db.close()

  testPath = (subkey, expectedPath) ->
    subkey.fullName.should.be.equal expectedPath
    subkey.path().should.be.equal expectedPath
  it "should get root subkey from db", ->
    @root.fullName.should.be.equal PATH_SEP
    @root.pathAsArray().should.be.deep.equal []
    result = @db.cache.get @root.fullName
    result.should.be.equal @root
    result.should.be.equal @db.root()
  describe ".parent()", ->
    it "should be null for root's parent", ->
      should.not.exist @root.parent()
    it "should get subkey's parent", ->
      subkey = @root.subkey('test')
      child = subkey.path('child')
      testPath subkey, '/test'
      testPath child, '/test/child'
      @db.cache.isExists('/test').should.be.true
      @db.cache.isExists('/test/child').should.be.true
      child.parent().should.be.equal subkey
      subkey.free()
      @db.cache.isExists('/test').should.be.false
      @db.cache.isExists('/test/child').should.be.false
    it "should raise error if subkey's parent not found in cache", ->
      child = @root.path('myparent/god/child')
      testPath child, '/myparent/god/child'
      should.throw child.parent.bind(child)
      myparent = @root.path('myparent')
      testPath myparent, '/myparent'
      should.throw child.parent.bind(child)
      child.free()
      myparent.free()
    it "should get subkey's latest parent if latestParent is true and it is not in cache", ->
      child = @root.path('myparent/god/child')
      testPath child, '/myparent/god/child'
      parent = child.parent latestParent:true
      parent.should.be.equal @root
      myparent = @root.path('myparent')
      testPath myparent, '/myparent'
      parent = child.parent latestParent:true
      parent.should.be.equal myparent
      child.free()
      myparent.free()
    it "should get subkey's latest parent via callback if it's is not in cache", (done)->
      child = @root.createPath('myparent/god/child')
      testPath child, '/myparent/god/child'
      parent = child.parent latestParent:true, (err, result)=>
        should.not.exist err
        result.should.be.equal @root
        myparent = @root.createPath('myparent')
        testPath myparent, '/myparent'
        parent = child.parent latestParent:true, (err, result)->
          result.should.be.equal myparent
          myparent.free()
          child.free()
          child.RefCount.should.be.equal 0
          done()
    it "should get subkey's parent even it's not in cache when createIfMissing", ->
      child = @root.createPath('myparent/god/child')
      child.RefCount.should.be.equal 1
      testPath child, '/myparent/god/child'
      # this parent is not exists, so createIfMissing:
      parent = child.parent createIfMissing: true
      testPath parent, '/myparent/god'
      p2 = @root.createPath('/myparent/god')
      p2.should.be.equal parent
      child.free()
      parent.free()
      p2.free()
      p2.RefCount.should.be.equal 0
      p2.free()
      p2.isDestroyed().should.be.equal true
    it "should get subkey's parent via callback even it's not in cache when createIfMissing", (done)->
      child = @root.path('myparent/god/child')
      testPath child, '/myparent/god/child'
      @db.cache.has('/myparent/god').should.be.false
      parent = child.parent createIfMissing: true, (err, result)=>
        should.not.exist err
        testPath result, '/myparent/god',
        @root.createPath('/myparent/god').should.be.equal result
        result.RefCount.should.be.equal 2
        result.destroy()
        result.isDestroyed().should.be.equal true
        child.free()
        @db.cache.has('/myparent/god').should.be.false
        @db.cache.has('/myparent/god/child').should.be.false
        done()
    it "should get subkey's parent via callback when createIfMissing", (done)->
      child = @root.path('myparent/god/child')
      testPath child, '/myparent/god/child'
      parent = @root.createPath 'myparent/god'
      child.parent createIfMissing: true, (err, result)=>
        should.not.exist err
        testPath result, '/myparent/god',
        result.should.be.equal parent
        result.should.be.equal @root.createPath('/myparent/god')
        result.destroy()
        child.free()
        @db.cache.has('/myparent/god').should.be.false
        @db.cache.has('/myparent/god/child').should.be.false
        done()
  describe ".setPath(path, callback)", ->
    it "should set myself to another path", ->
      subkey = @root.createPath('/my/subkey')
      testPath subkey, '/my/subkey'
      subkey.setPath('/my/other').should.be.true
      # setPath will remove itself from cache.
      @db.cache.isExists('/my/subkey').should.be.false
      testPath subkey, '/my/other'
      subkey.RefCount.should.be.equal 0
    it "should set myself to another path via callback", (done)->
      subkey = @root.createPath('/my/subkey')
      testPath subkey, '/my/subkey'
      subkey.setPath '/my/other', (err, result)=>
        should.not.exist err
        result.should.be.equal subkey
        # setPath will remove itself from cache.
        @db.cache.isExists('/my/subkey').should.be.false
        testPath result, '/my/other'
        done()
  describe ".path()/.fullName", ->
    it "should get myself path", ->
      subkey = @root.createPath('/my/subkey')
      testPath subkey, '/my/subkey'
  describe ".createPath(path)/.createSubkey(path)", ->
    before -> @subkey = @root.path 'myparent'
    after -> @subkey.free()
    it "should create subkey", ->
      key = @subkey.createPath('subkey1')
      testPath key, '/myparent/subkey1'
      key.free()

    it "should create many subkeys", ->
      keys = for i in [0...10]
        @subkey.createPath 'subkey'+i
      keys.should.have.length 10
      for key,i in keys
        testPath key, '/myparent/subkey'+i
        key.free()
      for key,i in keys
        key.RefCount.should.be.equal 0
    it "should create the same subkey more once", ->
      key = @subkey.createPath('subkey1')
      key.RefCount.should.be.equal 1
      testPath key, '/myparent/subkey1'
      keys = for i in [0...10]
        k = @subkey.createPath('subkey1')
        k.should.be.equal key
        k
      key.RefCount.should.be.equal keys.length+1
      for k in keys
        k.free()
      key.RefCount.should.be.equal 1
      key.free()
  describe ".path(path)/.subkey(path)", ->
    before -> @subkey = @root.path 'myparent'
    after -> @subkey.free()
    it "should get subkey", ->
      key = @subkey.path('subkey1')
      testPath key, '/myparent/subkey1'
      key.free()
      key.isDestroyed().should.be.equal true
    it "should get many subkeys", ->
      keys = for i in [0...10]
        @subkey.path 'subkey'+i
      keys.should.have.length 10
      for key,i in keys
        testPath key, '/myparent/subkey'+i
        key.free()
      for key,i in keys
        key.isDestroyed().should.be.true
    it "should get the same subkey more once", ->
      key = @subkey.path('subkey1')
      key.RefCount.should.be.equal 0
      testPath key, '/myparent/subkey1'
      keys = for i in [0...10]
        k = @subkey.path('subkey1')
        k.should.be.equal key
        k
      key.RefCount.should.be.equal 0
      key.free()
      key.isDestroyed().should.be.true
    it "should free subkeys after parent is freed ", ->
      parent = @subkey.path 'parent'
      keys = for i in [0...10]
        parent.path 'subkey'+i
      keys.should.have.length 10
      for key,i in keys
        testPath key, '/myparent/parent/subkey'+i
      parent.free()
      for key in keys
        assert.equal key.isDestroyed(), true
    it "should not free subkeys after parent is freed if pass free(false)", ->
      parent = @subkey.path 'parent'
      keys = for i in [0...10]
        parent.path 'subkey'+i
      keys.should.have.length 10
      for key,i in keys
        testPath key, '/myparent/parent/subkey'+i
      # pass false to do not free subkeys:
      parent.free(false)
      for key in keys
        assert.equal key.isDestroyed(), false
  describe "put operation", ->
    before -> @subkey = @root.path 'myputParent'
    after -> @subkey.free()
    it "should put key value via .putSync", ->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
    it "should put key value via .putAsync", (done)->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putAsync key, value, (err)=>
        should.not.exist err
        encodedKey = getEncodedKey @db, key, undefined, @subkey
        result = @db.data[encodedKey]
        result.should.be.equal getEncodedValue @db, value
        done()
    it "should put attribute via separator (.putSync)", ->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value, separator: '.'
      encodedKey = getEncodedKey @db, key, separator:'.', @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
    it "should put attribute (.putSync)", ->
      key = ".myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
    it "should put attribute via separator (.putAsync)", (done)->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putAsync key, value, separator:'.', (err)=>
        should.not.exist err
        encodedKey = getEncodedKey @db, key, separator:'.', @subkey
        result = @db.data[encodedKey]
        result.should.be.equal getEncodedValue @db, value
        done()
    it "should put attribute (.putAsync)", (done)->
      key = ".myput"+Math.random()
      value = Math.random()
      @subkey.putAsync key, value, (err)=>
        should.not.exist err
        encodedKey = getEncodedKey @db, key, undefined, @subkey
        result = @db.data[encodedKey]
        result.should.be.equal getEncodedValue @db, value
        done()
    it "should put another path key value via .putSync", ->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value, {path: 'hahe'}
      encodedKey = getEncodedKey @db, key, {path: 'hahe'}, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
    it "should put another path key value via .putAsync", (done)->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putAsync key, value, {path: 'hahe'}, (err)=>
        should.not.exist err
        encodedKey = getEncodedKey @db, key, {path: 'hahe'}, @subkey
        result = @db.data[encodedKey]
        result.should.be.equal getEncodedValue @db, value
        done()
    it "should put key value via .put", ->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.put key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
    it "should put key value via .put async", (done)->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.put key, value, (err)=>
        should.not.exist err
        encodedKey = getEncodedKey @db, key, undefined, @subkey
        result = @db.data[encodedKey]
        result.should.be.equal getEncodedValue @db, value
        done()
  describe "get operation", ->
    before -> @subkey = @root.path 'myGetParent'
    after -> @subkey.free()
    it "should get key .getSync", ->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      result = @subkey.getSync key
      result.should.be.equal value
    it "should get key .getAsync", (done)->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      @subkey.getAsync key, (err, result)=>
        should.not.exist err
        result.should.be.equal value
        done()
    it "should get attribute .getSync", ->
      key = ".myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      result = @subkey.getSync key
      result.should.be.equal value
      result = @subkey.getSync key.slice(1), separator:'.'
      result.should.be.equal value
    it "should get attribute via separator .getSync", ->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value, separator:'.'
      encodedKey = getEncodedKey @db, key, separator:'.', @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      result = @subkey.getSync key, separator:'.'
      result.should.be.equal value
      result = @subkey.getSync '.'+key
      result.should.be.equal value
    it "should get attribute .getAsync", (done)->
      key = ".myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      @subkey.getAsync key, (err, result)=>
        should.not.exist err
        result.should.be.equal value
        @subkey.getAsync key.slice(1), separator:'.', (err, result)=>
          should.not.exist err
          result.should.be.equal value
          done()
    it "should get attribute via separator .getAsync", (done)->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value, separator:'.'
      encodedKey = getEncodedKey @db, key, separator:'.', @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      @subkey.getAsync key, separator:'.', (err, result)=>
        should.not.exist err
        result.should.be.equal value
        @subkey.getAsync '.'+key, (err, result)=>
          should.not.exist err
          result.should.be.equal value
          done()
    it "should get another path key value via .getSync", ->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value, {path: 'hahe'}
      encodedKey = getEncodedKey @db, key, {path: 'hahe'}, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      result = @subkey.getSync key, {path: 'hahe'}
      result.should.be.equal value
    it "should get another path key value via .getAsync", (done)->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value, {path: 'hahe'}
      encodedKey = getEncodedKey @db, key, {path: 'hahe'}, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      @subkey.getAsync key, {path: 'hahe'}, (err, result)=>
        should.not.exist err
        result.should.be.equal value
        done()
    it "should get key value via .get", ->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.put key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      result = @subkey.get key
      result.should.be.equal value
    it "should get key value via .get async", (done)->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.put key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      @subkey.get key, (err, result)=>
        should.not.exist err
        result.should.be.equal value
        done()
  describe "del operation", ->
    before -> @subkey = @root.path 'myDelParent'
    after -> @subkey.free()
    it "should del key .delSync", ->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      result = @subkey.delSync key
      result.should.be.equal true
      result = @db.data[encodedKey]
      should.not.exist result
    it "should del key .delAsync", (done)->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      @subkey.delAsync key, (err, result)=>
        should.not.exist err
        result = @db.data[encodedKey]
        should.not.exist result
        done()
    it "should del attribute .delSync", ->
      key = ".myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      result = @subkey.delSync key
      result.should.be.equal true
      result = @db.data[encodedKey]
      should.not.exist result
    it "should del attribute via separator .delSync", ->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value, separator:'.'
      encodedKey = getEncodedKey @db, key, separator:'.', @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      result = @subkey.delSync key, separator:'.'
      result.should.be.equal true
      result = @db.data[encodedKey]
      should.not.exist result
    it "should del attribute .delAsync", (done)->
      key = ".myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      @subkey.delAsync key, (err, result)=>
        should.not.exist err
        result = @db.data[encodedKey]
        should.not.exist result
        done()
    it "should del attribute via separator .delAsync", (done)->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value, separator:'.'
      encodedKey = getEncodedKey @db, key, separator:'.', @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      @subkey.delAsync key, separator:'.', (err, result)=>
        should.not.exist err
        result = @db.data[encodedKey]
        should.not.exist result
        done()
    it "should del another path key value via .delSync", ->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value, {path: 'hahe'}
      encodedKey = getEncodedKey @db, key, {path: 'hahe'}, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      result = @subkey.delSync key, {path: 'hahe'}
      result.should.be.equal true
      result = @db.data[encodedKey]
      should.not.exist result
    it "should del another path key value via .delAsync", (done)->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.putSync key, value, {path: 'hahe'}
      encodedKey = getEncodedKey @db, key, {path: 'hahe'}, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      @subkey.delAsync key, {path: 'hahe'}, (err, result)=>
        should.not.exist err
        result = @db.data[encodedKey]
        should.not.exist result
        done()
    it "should del key value via .del", ->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.put key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      result = @subkey.del key
      result.should.be.equal true
      result = @db.data[encodedKey]
      should.not.exist result
    it "should del key value via .del async", (done)->
      key = "myput"+Math.random()
      value = Math.random()
      @subkey.put key, value
      encodedKey = getEncodedKey @db, key, undefined, @subkey
      result = @db.data[encodedKey]
      result.should.be.equal getEncodedValue @db, value
      @subkey.del key, (err, result)=>
        should.not.exist err
        result = @db.data[encodedKey]
        should.not.exist result
        done()
  describe "batch operation", ->
    before -> @subkey = @root.path 'myBatchParent'
    after -> @subkey.free()
    genOps = (separator='', count=10)->
      for i in [1..count]
        key: separator+'key'+Math.random()
        value: Math.random()
    testOps = (ops, options) ->
      #console.log 'data', @db.data
      for op in ops
        encodedKey = getEncodedKey @db, op.key, options, @subkey
        result = @db.data[encodedKey]
        result.should.be.equal getEncodedValue @db, op.value
    it ".batchSync", ->
      ops = genOps()
      @subkey.batchSync ops
      testOps.call @, ops, undefined
    it ".batchAsync", (done)->
      ops = genOps()
      @subkey.batchAsync ops, (err)=>
        should.not.exist err
        testOps.call @, ops
        done()
    it "should batch attribute via separator (.batchSync)", ->
      ops = genOps()
      @subkey.batchSync ops, separator:'.'
      testOps.call @, ops, separator:'.'
    it "should batch attribute (.batchSync)", ->
      ops = genOps('.')
      @subkey.batchSync ops
      testOps.call @, ops
    it "should batch attribute via separator (.batchAsync)", (done)->
      ops = genOps()
      @subkey.batchAsync ops, separator:'.', (err)=>
        should.not.exist err
        testOps.call @, ops, separator:'.'
        done()
    it "should batch attribute (.batchAsync)", (done)->
      ops = genOps('.')
      @subkey.batchAsync ops, (err)=>
        should.not.exist err
        testOps.call @, ops
        done()
    it "should batch another path key value via .batchSync", ->
      ops = genOps()
      @subkey.batchSync ops, path: 'hahe'
      testOps.call @, ops, path: 'hahe'
    it "should batch another path key value via .batchAsync", (done)->
      ops = genOps()
      @subkey.batchAsync ops, path: 'hahe', (err)=>
        should.not.exist err
        testOps.call @, ops, path: 'hahe'
        done()
    it ".batch", ->
      ops = genOps()
      @subkey.batch ops, path: 'hahe'
      testOps.call @, ops, path: 'hahe'
    it ".batch async", (done)->
      ops = genOps()
      @subkey.batchAsync ops, path: 'hahe', (err)=>
        should.not.exist err
        testOps.call @, ops, path: 'hahe'
        done()


