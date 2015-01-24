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
EncodingIterator= require("encoding-iterator")
inherits        = require 'abstract-object/lib/util/inherits'
isInheritedFrom = require 'abstract-object/lib/util/isInheritedFrom'
isObject        = require 'abstract-object/lib/util/isObject'
FakeDB          = require './fake-nosql'
codec           = require '../src/codec'
path            = require '../src/path'

setImmediate          = setImmediate || process.nextTick
InvalidArgumentError  = Errors.InvalidArgumentError
NotFoundError         = Errors.NotFoundError
PATH_SEP              = codec.PATH_SEP
SUBKEY_SEP            = codec.SUBKEY_SEP
_encodeKey            = codec._encodeKey
toPath                = path.join

chai.use(sinonChai)

FakeDB = SubkeyNoSQL(FakeDB)

describe "Add Subkey to a NoSQL Class", ->
  it "should add subkey feature to a NoSQL Database", ->
    #isInheritedFrom(FakeDB, SubkeyNoSQL).should.not.be.true
    #SubkeyNoSQL(FakeDB)
    isInheritedFrom(FakeDB, SubkeyNoSQL).should.be.equal FakeDB
  it "should raise error when adding a illegal NoSQL class", ->
    class IllegalDB
    should.throw SubkeyNoSQL.bind(null, IllegalDB), 'class should be inherited from'

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
  _opts = {}
  data.sort (a,b)->
    _opts.keyEncoding = a.keyEncoding
    _opts.keyEncoding = opts.keyEncoding if opts and not _opts.keyEncoding
    keyEncoding = db.keyEncoding _opts
    a = if keyEncoding then keyEncoding.encode a.key else a.key
    _opts.keyEncoding = b.keyEncoding
    _opts.keyEncoding = opts.keyEncoding if opts and not _opts.keyEncoding
    keyEncoding = db.keyEncoding _opts
    b = if keyEncoding then keyEncoding.encode b.key else b.key
    return 1 if a > b
    return -1 if a < b
    return 0
  data

getEncodedKey = (db, key, options, parentPath) ->
  _encodeKey db.getPathArray(options, parentPath), key, db.keyEncoding(options), options
getEncodedOps = (db, ops, opts) ->
  vParentPath = opts.path if opts
  ops.slice().map (op) ->
    key: getEncodedKey db, op.key, op, vParentPath
    value: if op.value then JSON.stringify op.value else op.value
    type: op.type
    path: path = db.getPathArray(op, vParentPath)
    _keyPath: [path, op.key]

describe "SubkeyNoSQL", ->
  before ->
    @db = new FakeDB()
  after ->
    @db.close()

  testOpen = (db)->
    should.exist db.SubkeyClass, "db.SubkeyClass"
    should.exist db.preHooks, "db.preHooks"
    should.exist db.postHooks, "db.postHooks"
    should.exist db.cache, "db.cache"
  describe ".open", ->
    afterEach ->
      @db.close()
    it "should set string encoding", ->
      @db.open({keyEncoding:'json', valueEncoding: 'text'})
      @db._options.keyEncoding.should.be.equal Codec('json')
      @db._options.valueEncoding.should.be.equal Codec('text')
      testOpen @db
    it "should set encoding instance", ->
      json = new Codec['json']
      @db.open({keyEncoding:json})
      @db._options.keyEncoding.should.be.equal json
      should.not.exist @db._options.valueEncoding
      testOpen @db
    it "should set undefined encoding when open unknown encoding", ->
      @db.open({keyEncoding:'Not FOund', valueEncoding: 'No SUch'})
      should.not.exist @db._options.keyEncoding
      should.not.exist @db._options.valueEncoding
      testOpen @db
    it "should set options.path", ->
      @db.open({path:'/test'})
      @db._pathArray.should.be.deep.equal ['test']
      testOpen @db
  describe ".getPathArray", ->
    afterEach ->
      @db.close()
    it "should get self path", ->
      @db.open({path:'test'})
      @db.getPathArray().should.be.deep.equal ['test']
    it "should get root path via defaults", ->
      @db.open({})
      @db.getPathArray().should.be.deep.equal []
    it "should get path", ->
      @db.open path: 'test'
      @db.getPathArray(path:'a').should.be.deep.equal ['test', 'a']

  describe ".isExistsSync", ->
    it "should encode key", ->
      @db.open({keyEncoding:'json', path: toPath 'root', 'path1'})
      expectedKey = {myKeyName: 12345}
      @db.isExistsSync expectedKey
      #expectedKey = codec.encodeKey
      expectedKey = "/root/path1"+ SUBKEY_SEP + JSON.stringify expectedKey
      @db._isExistsSync.should.have.been.calledWith expectedKey
      @db._isExistsSync.should.have.been.calledOnce
    it "should encode key with options.path", ->
      @db.open({keyEncoding:'json', path: toPath 'root', 'path1'})
      expectedKey = {myKeyName: 12345}
      @db._isExistsSync.reset()
      @db.isExistsSync expectedKey, path: 'other'
      #expectedKey = codec.encodeKey
      expectedKey = toPath(PATH_SEP,"root","path1", "other") + SUBKEY_SEP + JSON.stringify expectedKey
      @db._isExistsSync.should.have.been.calledWith expectedKey
      @db._isExistsSync.should.have.been.calledOnce

  describe ".isExists", ->
    it "should encode key sync", ->
      @db.open({keyEncoding:'json', path: toPath 'root', 'path1'})
      expectedKey = {myKeyName: 12345}
      @db._isExistsSync.reset()
      @db.isExists expectedKey
      expectedKey = "/root/path1"+ SUBKEY_SEP + JSON.stringify expectedKey
      @db._isExistsSync.should.have.been.calledWith expectedKey
      @db._isExistsSync.should.have.been.calledOnce
    it "should encode key async", (done)->
      @db.open({keyEncoding:'json', path: toPath 'root', 'path1'})
      expectedKey = {myKeyName: 12345}
      @db._isExistsSync.reset()
      @db.isExists expectedKey, (err, result)=>
        expectedKey = "/root/path1"+ SUBKEY_SEP + JSON.stringify expectedKey
        should.not.exist err
        @db._isExistsSync.should.have.been.calledWith expectedKey
        @db._isExistsSync.should.have.been.calledOnce
        done()
    it "should encode key with options.path async", (done)->
      @db.open({keyEncoding:'json', path: toPath 'root', 'path1'})
      expectedKey = {myKeyName: 12345}
      @db._isExistsSync.reset()
      @db.isExists expectedKey, {path: 'other'}, (err, result)=>
        expectedKey = "/root/path1/other"+ SUBKEY_SEP + JSON.stringify expectedKey
        should.not.exist err
        @db._isExistsSync.should.have.been.calledWith expectedKey
        @db._isExistsSync.should.have.been.calledOnce
        done()

  describe ".getBuffer", ->
    describe ".getBufferSync", ->
      it "should encode key", ->
        @db.open({keyEncoding:'json', path: 'root'})
        expectedKey = myKeyName: Math.random()
        @db._getBufferSync.reset()
        should.throw @db.getBufferSync.bind @db, expectedKey, NotFoundError
        expectedKey = getEncodedKey @db, expectedKey
        @db._getBufferSync.should.have.been.calledWith expectedKey
        @db._getSync.should.have.been.calledWith expectedKey
    describe ".getBuffer", ->
      it "should encode key sync", ->
        @db.open({keyEncoding:'json'})
        expectedKey = myKeyName: Math.random()
        @db._getBufferSync.reset()
        should.throw @db.getBuffer.bind @db, expectedKey, NotFoundError
        expectedKey = getEncodedKey @db, expectedKey
        @db._getBufferSync.should.have.been.calledWith expectedKey
      it "should encode key async", (done)->
        @db.open({keyEncoding:'json', path: 'root'})
        expectedKey = myKeyName: Math.random()
        @db._getBufferSync.reset()
        opts = path: 'other'
        @db.getBuffer expectedKey, null, opts, (err, result)=>
          expectedKey = getEncodedKey @db, expectedKey, opts
          should.exist err
          err.notFound().should.be.true
          @db._getBufferSync.should.have.been.calledWith expectedKey
          done()

  describe ".mGetSync", ->
    afterEach -> @db.close()
    it "should encode key, decode value", ->
      @db.open({keyEncoding:'json', valueEncoding: 'json', path: 'root'})
      data = genData(@db, 'mget')
      keys = for i in data
        i.key
      result = @db.mGetSync keys, path: 'mget'
      result.should.have.length keys.length
      for o,i in result
        o.key.should.be.deep.equal data[i].key
        o.value.should.be.deep.equal data[i].value
    it "should encode key only", ->
      @db.open({keyEncoding:'json'})
      data = genData(@db, 'mget')
      keys = for i in data
        i.key
      result = @db.mGetSync keys, path: 'mget'
      result.should.have.length keys.length
      for o,i in result
        o.key.should.be.deep.equal data[i].key
        o.value.should.be.deep.equal data[i].value
    it "should decode value array", ->
      @db.open({keyEncoding:'json', valueEncoding: 'json'})
      data = genData(@db, 'mget')
      keys = for i in data
        i.key
      result = @db.mGetSync keys, path: 'mget', keys: false
      result.should.have.length keys.length
      for o,i in result
        o.should.be.deep.equal data[i].value

  describe ".mGet", ->
    it "should encode key sync", ->
      @db.open({keyEncoding:'json', valueEncoding: 'json'})
      data = genData(@db, 'mget')
      keys = for i in data
        i.key
      result = @db.mGet keys, path: 'mget'
      result.should.have.length keys.length
      for o,i in result
        o.key.should.be.deep.equal data[i].key
        o.value.should.be.deep.equal data[i].value
    it "should encode key only sync", ->
      @db.open({keyEncoding:'json'})
      data = genData(@db, 'mget')
      keys = for i in data
        i.key
      result = @db.mGet keys, path: 'mget'
      result.should.have.length keys.length
      for o,i in result
        o.key.should.be.deep.equal data[i].key
        o.value.should.be.deep.equal data[i].value
    it "should decode value array sync", ->
      @db.open({keyEncoding:'json', valueEncoding: 'json'})
      data = genData(@db, 'mget')
      keys = for i in data
        i.key
      result = @db.mGet keys, path: 'mget', keys: false
      result.should.have.length keys.length
      for o,i in result
        o.should.be.deep.equal data[i].value
    it "should encode key, decode value async", (done)->
      @db.open({keyEncoding:'json', valueEncoding: 'json'})
      data = genData(@db, 'mget')
      keys = for i in data
        i.key
      @db.mGet keys, path: 'mget', (err, result)=>
        should.not.exist err
        result.should.have.length keys.length
        for o,i in result
          o.key.should.be.deep.equal data[i].key
          o.value.should.be.deep.equal data[i].value
        done()
    it "should encode key only async", (done)->
      @db.open keyEncoding:'json'
      data = genData(@db, 'mget')
      keys = for i in data
        i.key
      @db.mGet keys, path: 'mget', (err, result)=>
        should.not.exist err
        result.should.have.length keys.length
        for o,i in result
          o.key.should.be.deep.equal data[i].key
          o.value.should.be.deep.equal data[i].value
        done()
    it "should decode value array async", (done)->
      @db.open({keyEncoding:'json', valueEncoding: 'json'})
      data = genData(@db, 'mget')
      keys = for i in data
        i.key
      @db.mGet keys, path:'mget',keys:false, (err, result)=>
        should.not.exist err
        result.should.have.length keys.length
        for o, i in result
          o.should.be.deep.equal data[i].value
        done()

  describe ".getSync", ->
    beforeEach -> @db.open({keyEncoding:'json', valueEncoding: 'json', path: 'root'})
    afterEach  -> @db.close()

    it "should encode key", ->
      expectedKey = myKeyName: Math.random()
      @db.put expectedKey, expectedKey
      result = @db.getSync expectedKey
      result.should.be.deep.equal expectedKey
      expectedKey = getEncodedKey @db, expectedKey
      @db._getSync.should.have.been.calledWith expectedKey
    it "should decode value", ->
      expectedKey = myKeyName: Math.random()
      expectedValue = [mv: Math.random(), k: "123"]
      @db.put expectedKey, expectedValue
      result = @db.getSync expectedKey
      result.should.be.deep.equal expectedValue
      expectedKey = getEncodedKey @db, expectedKey
  describe ".get", ->
    beforeEach -> @db.open({keyEncoding:'json', valueEncoding: 'json', path: 'root'})
    afterEach  -> @db.close()
    it "should encode key sync", ->
      expectedKey = myKeyName: Math.random()
      @db.put expectedKey, expectedKey
      result = @db.get expectedKey
      result.should.be.deep.equal expectedKey
      expectedKey = getEncodedKey @db, expectedKey
      @db._getSync.should.have.been.calledWith expectedKey
    it "should decode value sync", ->
      expectedKey = myKeyName: Math.random()
      expectedValue = [mv: Math.random(), k: "123"]
      @db.put expectedKey, expectedValue
      result = @db.get expectedKey
      result.should.be.deep.equal expectedValue
      expectedKey = getEncodedKey @db, expectedKey
    it "should encode key async", (done)->
      expectedKey = myKeyName: Math.random()
      @db.put expectedKey, expectedKey
      @db.get expectedKey, (err, result)=>
        should.not.exist err
        result.should.be.deep.equal expectedKey
        expectedKey = getEncodedKey @db, expectedKey
        @db._getSync.should.have.been.calledWith expectedKey
        done()
    it "should decode value async", (done)->
      expectedKey = myKeyName: Math.random()
      expectedValue = [mv: Math.random(), k: "123"]
      @db.put expectedKey, expectedValue
      @db.get expectedKey, (err, result)=>
        should.not.exist err
        result.should.be.deep.equal expectedValue
        expectedKey = getEncodedKey @db, expectedKey
        @db._getSync.should.have.been.calledWith expectedKey
        done()

  describe ".putSync", ->
    it "should encode key,value", ->
      @db.open({keyEncoding:'json', valueEncoding: 'json'})
      expectedKey   = myKeyName: Math.random()
      expectedValue = myValueName: Math.random()
      @db.putSync expectedKey, expectedValue
      expectedKey = getEncodedKey @db, expectedKey
      expectedValue = JSON.stringify expectedValue
      @db._putSync.should.have.been.calledWith expectedKey, expectedValue
  describe ".put", ->
    it "should encode key,value sync", ->
      @db.open({keyEncoding:'json', valueEncoding: 'json'})
      expectedKey = myKeyName: Math.random()
      expectedValue = myValueName: Math.random()
      @db.putSync expectedKey, expectedValue
      expectedValue = JSON.stringify expectedValue
      expectedKey = getEncodedKey @db, expectedKey
      @db._putSync.should.have.been.calledWith expectedKey, expectedValue
    it "should encode key,value async", (done)->
      @db.open({keyEncoding:'json', valueEncoding: 'json'})
      expectedKey = myKeyName: Math.random()
      expectedValue = myValueName: Math.random()
      @db.put expectedKey, expectedValue, (err, result)=>
        should.not.exist err
        expectedKey = getEncodedKey @db, expectedKey
        expectedValue = JSON.stringify expectedValue
        @db._putSync.should.have.been.calledWith expectedKey, expectedValue
        done()

  describe ".delSync", ->
    it "should encode key", ->
      @db.open({keyEncoding:'json', valueEncoding: 'json'})
      expectedKey = myKeyName: Math.random()
      @db._delSync.reset()
      @db.delSync expectedKey
      expectedKey = getEncodedKey @db, expectedKey
      @db._delSync.should.have.been.calledWith expectedKey
  describe ".del", ->
    it "should encode key sync", ->
      @db.open({keyEncoding:'json', valueEncoding: 'json'})
      expectedKey = myKeyName: Math.random()
      @db._delSync.reset()
      @db.del expectedKey
      expectedKey = getEncodedKey @db, expectedKey
      @db._delSync.should.have.been.calledWith expectedKey
    it "should encode key async", (done)->
      @db.open({keyEncoding:'json', valueEncoding: 'json'})
      expectedKey = myKeyName: Math.random()
      @db._delSync.reset()
      @db.del expectedKey, (err, result)=>
        should.not.exist err
        expectedKey = getEncodedKey @db, expectedKey
        @db._delSync.should.have.been.calledWith expectedKey
        done()

  allOperations = [
    type: "put"
    path: "op"
    key: [Math.random()]
    value: v:Math.random()
  ,
    type: "del"
    key: ['delKey']
  ]
  describe ".batchSync", ->
    beforeEach -> @db.open({keyEncoding:'json', valueEncoding: 'json', path: 'root'})
    afterEach  -> @db.close()
    it "should raise error on invalid arugments", ->
      should.throw @db.batchSync.bind(@db), InvalidArgumentError
    it "should encode key", ->
      operations = allOperations.slice()
      @db.batchSync operations
      @db._batchSync.should.have.been.calledWith operations
  describe ".batch", ->
    beforeEach -> @db.open({keyEncoding:'json', valueEncoding: 'json', path: 'root'})
    afterEach  -> @db.close()
    it "should get error on invalid arugments", ->
      @db.batch undefined, (err)->
        should.exist err
        err.invalidArgument().should.be.true
    it "should encode key sync", ->
      @db.putSync ['delKey'], 'something'
      @db.isExistsSync(['delKey']).should.be.true
      @db.isExistsSync(allOperations[0].key, allOperations[0]).should.not.be.true
      operations = allOperations.slice()
      expectedOps = getEncodedOps @db, operations
      @db._batchSync.reset()
      @db.batch operations
      @db._batchSync.should.have.been.calledOnce
      @db._batchSync.should.have.been.calledWith operations
      @db.isExistsSync(['delKey']).should.not.be.true
      @db.isExistsSync(allOperations[0].key, allOperations[0]).should.be.true
      data = {}
      data[expectedOps[0].key] = expectedOps[0].value
      data.should.be.deep.equal @db.data
    it "should encode key async", (done)->
      @db.open({keyEncoding:'json', valueEncoding: 'json'})
      operations = allOperations.slice()
      @db.batch operations, (err, result)=>
        should.not.exist err
        @db._batchSync.should.have.been.calledWith operations
        done()

  describe ".iterator", ->
    beforeEach -> @db.open({keyEncoding:'json', valueEncoding: 'json', path: 'root'})
    afterEach  -> @db.close()
    it "should encode range", ->
      gt = {key:123}
      lt = {key:12}
      iterator = @db.iterator lt: lt, gt: gt
      iterator.options.should.have.property "lt", getEncodedKey @db, lt
      iterator.options.should.have.property "gt", getEncodedKey @db, gt
      iterator = @db.iterator start: gt, end: lt
      iterator.options.should.have.property "gte", getEncodedKey @db, gt
      iterator.options.should.have.property "lte", getEncodedKey @db, lt
      iterator = @db.iterator gte: gt, lte: lt
      iterator.options.should.have.property "gte", getEncodedKey @db, gt
      iterator.options.should.have.property "lte", getEncodedKey @db, lt
    it "should encode range keys", ->
      keys = [{ikey:Math.random()}, {ikey:Math.random()}]
      expectedKeys = keys.map (i)=>getEncodedKey @db, i
      iterator = @db.iterator range: keys
      iterator.options.range.should.be.deep.equal expectedKeys
    it "should decode nextSync result", ->
      data = genData @db
      iterator = @db.iterator()
      i = -1
      while ++i < data.length
        result = iterator.nextSync()
        result.should.be.deep.equal key:data[i].key, value:data[i].value
      iterator.nextSync().should.be.false
    it "should decode next result", (done)->
      data = genData @db
      iterator = @db.iterator()
      i = 0
      nextOne = ->
        iterator.next (err, key, value)->
          should.not.exist err
          key.should.be.deep.equal data[i].key
          value.should.be.deep.equal data[i].value
          if ++i < 10
            nextOne()
          else 
            overCallbackCount = 0
            iterator.next (err, key, value)->
              overCallbackCount++
              overCallbackCount.should.be.equal 1
              should.exist err
              err.notFound().should.be.true
              should.not.exist key
              should.not.exist value
              done()
      nextOne()
    it "should decode nextSync range keys result", ->
      data = genData @db, 'it'
      expectedKeys = []
      keys = for i in data
        expectedKeys.push
          key: i.key
          value: i.value
        i.key
      iterator = @db.iterator range:keys, path:'it'
      i = 0
      result = []
      while i < keys.length
        result.push iterator.nextSync()
        i++

      result.should.be.deep.equal expectedKeys
    it "should decode next range keys result", (done)->
      data = genData @db, 'it'
      expectedKeys = []
      keys = for i in data
        expectedKeys.push
          key: i.key
          value: i.value
        i.key
      iterator = @db.iterator range:keys, path:'it'
      result=[]
      nextOne= ->
        iterator.next (err, key, value)->
          should.not.exist err
          result.push
            key:key
            value:value
          if result.length < keys.length
            nextOne()
          else
            result.should.be.deep.equal expectedKeys
            done()
      nextOne()
  describe ".pre Hook", ->
  describe ".post Hook", ->
