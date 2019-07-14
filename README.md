## nosql-subkey [![npm](https://img.shields.io/npm/v/nosql-subkey.svg)](https://npmjs.org/package/nosql-subkey)

[![Build Status](https://img.shields.io/travis/snowyu/node-nosql-subkey/master.svg)](http://travis-ci.org/snowyu/node-nosql-subkey) 
[![downloads](https://img.shields.io/npm/dm/nosql-subkey.svg)](https://npmjs.org/package/nosql-subkey) 
[![license](https://img.shields.io/npm/l/nosql-subkey.svg)](https://npmjs.org/package/nosql-subkey) 


Add the subkey/sublevel feature to [abstract-nosql](https://github.com/snowyu/node-abstract-nosql) database.

the subkey feature is a special [encoding](https://github.com/snowyu/node-nosql-encoding) feature.
It's inherited from [nosql-encoding](https://github.com/snowyu/node-nosql-encoding).

## Purpose

* Dynamic sublevels via key path
* Hierarchy Key/Value data store like file path.
+ minimatch supports for hook and search.
+ Stream support with [nosql-stream](https://github.com/snowyu/node-nosql-stream)
+ Encoding supports
+ hookable to put/del.
+ destroy event on Subkey
  * it will be trigger when subkey.free()
* the Subkey instance lifecycle state manage.
 * object state(\_obj_state):
   * initing: the object is initing.
   * inited: the object is created.
   * destroying: the object is destroying(before destroy).
   * destroyed: the object is destroyed(after destroy). the destroyed event will be emitted.
 * object loading state(\_loading_state):
   * unload: the object is not loaded from database.
   * loading: the object is loading from database.
   * loaded: the object has already been loaded from database.
     * dirtied: the object has been modified, but not saved to database yet.
       * triggered the dirtied event, the operation is an object item(see batch): {type:"put", key:keyName, value:value}
         on "dirtied", (keyObj, operation)->
     * modifying: the object has been modified to database, but not loaded to the object(affect the loading state to loading)
     * modified: the object has been modified to database(not affect the loading state).
     * deleted: the object has been deleted from database(affect the object state to destroyed).
+ LRU-cache subkey supports

## todo

+ index the integer and json object key on some subkey.
  * mechanism:1
    + customize precodec in subkey()'s options
      + codec option: bytewise
    + store the ".codec" attribute to subkey.
    * disadvantage: performance down
    * advantage: more flexible codec.
  * mechanism:2
    * extent the current codec to support index integer and json object
    * advantage: .
    * disadvantage: performance down a little, key human-readable down a little.
      * the integer and json object can not be readable.


## Main Concepts

The key is always string only unless it's an index.

* Key
  * Key Path: like hierarchical file path.
  * Subkey: a key can have a lot of subkeys.
  * alias
* Value
  * can not be undefined, it used as deleted.
  * can be null.

## Stability

Unstable: Expect patches and features, possible api changes.

This module is working well, but may change in the future as its use is further explored.

## Usage

```js
var addSubkeyFeatureTo = require('nosql-subkey')
var addStreamFeatureTo = require('nosql-stream')
var MemDB = addStreamFeatureTo(addSubkeyFeatureTo(require('nosql-memdb')))

var db = new MemDB()
db.open()

root = db.root()

//u should free these .createSubkey() items:
var stuff = root.createSubkey('stuff') 
var animal = stuff.createSubkey('animal')
// .subkey() items will be freed when stuff free.
var plant = stuff.subkey('plant')

//put a key into animal!
animal.put("pig", value, function () {})

//new dynamic hierarchy data storage usage:
animal.put("../plant/cucumber", value, function (err) {})
root.put("/stuff/animal/pig", value, function(err){})
root.get("/stuff/animal/pig", function(err, value){})

//put pig's attribute as key/value
root.put("/stuff/animal/pig/.mouth", value, function(err){})
root.put("/stuff/animal/pig/.ear", value, function(err){})

//list all pig's attributes
root.createReadStream({path: "/stuff/animal/pig", separator="."})
//return: {".mouth":value, ".ear":value}

//list all pig's path(excludes the subkeys)
//it will search from "/stuff/\x00" to "/stuff/\uffff"
root.createPathStream({path: "/stuff"}) //= db.createReadStream({separator:'/', separatorRaw: true, start:'0'})
//return:{ 'animal/pig': value, 'animal/pig.ear': value, 'animal/pig.mouth': value, 'plant/cucumber': value}


//list all keys in "/stuff/animal"
root.createReadStream({path: "/stuff/animal"})

//list all keys in "/stuff/plant"
animal.createReadStream({start: "../plant"})


//write by stream
var wsAnimal = animal.createWriteStream()
wsAnimal.on('err', function(err){throw err})
wsAnimal.on('close', function(){})
wsAnimal.write({key: "cow", value:value})
wsAnimal.write({key: "/stuff/animal/cow", value:value})
wsAnimal.write({key: "../plant/tomato", value:value})
wsAnimal.end()

//crazy usage:
//the path will always be absolute key path.
//Warning: setPath will be broken the subkeys cache on nut!!
//  if setPath it will remove itself from cache.
animal.setPath("/stuff/plant")
animal.setPath(plant)
//now the "animal" is plant in fact.
animal.get("cucumber", function(err, value){})

```

## API

### DB/Subkey.subkey()/path()

Get a specified Subkey instance. It will remove from cache if you free it.
this instance will be freed if its parent is freed.

please use the createSubkey if u wanna keep the instance even parent is freed.

* Subkey.subkey(keyPath, options, readyCallback)
  * = Subkey.path(keyPath, options, readyCallback)
* Subkey.subkey(keyPath, readyCallback)
  * = Subkey.path(keyPath, readyCallback)

__arguments__ see createSubkey below:


### DB/Subkey.createSubkey()/createPath()

Create a new Subkey instance. you should free it when no used.

* Subkey.createSubkey(keyPath, options, readyCallback)
  * = Subkey.createPath(keyPath, options, readyCallback)
* Subkey.createSubkey(keyPath, readyCallback)
  * = Subkey.createPath(keyPath, readyCallback)


__arguments__

* keyPath: the key path can be a relative or absolute path.
* options: the options object is optional.
  * loadValue: boolean, defalut is true. whether load the value of the key after the key is created.
  * forceCreate: boolean, defalut is false. whether ignore the global cache always create a new Subkey instance.
    which means it will bypass the global cache if it is true.
  * addRef: boolean, defalut is true. whether add a reference count to the key instance in the global cache.
    * only free when RefCount is less than zero.
* readyCallback: triggered when loading finished.
  * function readyCallback(err, theKey)
    * theKey may be set even though the error occur

__return__

* object: the Subkey instance object


The usages:

* Isolate the key like data tables, see also [level-sublevel](https://github.com/dominictarr/level-sublevel).
* Key/Value ORM: Mapping the Key/Value to an Object with subkeys supports.
* Hierarchical Key/Value Storage


## Subkey.fullName/path()

* Subkey.fullName
* Subkey.path()

__arguments__

* None

__return__

* String: return the subkey's full path.

## Subkey.readStream/createReadStream([options])

create a read stream to visit the child subkeys of this subkey.

* Subkey.readStream()
* Subkey.readStream(options)

__arguments__

* options: this options object is optional argument.
  * `'path'` *(string|Subkey Object)*: can be relative or absolute key path or another subkey object to search
  * `'separator'` *(char)*
  * `'bounded'` *(boolean, default: `true`)*: whether limit the boundary to this subkey only.
    * through that can limit all keys are the subkey's children. So DONT disable it unless you know why.
  * `'separatorRaw'` *(boolean, default: `false`)*: do not convert the separator, use the separator directly if true.
    * see also: 'Internal Storage Format for Key'
    * in fact the pathStream is set the options to {separator:'/', separatorRaw: true, start:'0'} simply.
  * `'next'`: the raw key data to ensure the readStream return keys is greater than the key. See `'last'` event.
    * note: this will affect the range[gt/gte or lt/lte(reverse)] options.
  * `'filter'` *(function)*: to filter data in the stream
    * function filter(key, value) if return:
      *  0(consts.FILTER_INCLUDED): include this item(default)
      *  1(consts.FILTER_EXCLUDED): exclude this item.
      * -1(consts.FILTER_STOPPED): stop stream.
    * note: the filter function argument 'key' and 'value' may be null, it is affected via keys and values of this options.
  * `'range'` *(string or array)*: the keys are in the give range as the following format:
    * string:
      * "[a, b]": from a to b. a,b included. this means {gte:'a', lte: 'b'}
      * "(a, b]": from a to b. b included, a excluded. this means {gt:'a', lte:'b'}
      * "[, b)" : from begining to b, begining included, b excluded. this means {lt:'b'}
      * "(, b)" : from begining to b, begining excluded, b excluded. this means {gt:null, lt:'b'}
      * note: this will affect the gt/gte/lt/lte options.
        * "(,)": this is not be allowed. the ending should be a value always.
    * array: the key list to get. eg, ['a', 'b', 'c']
      * `gt`/`gte`/`lt`/`lte` options will be ignored.
  * `'gt'` (greater than), `'gte'` (greater than or equal) define the lower bound of the range to be streamed. Only records where the key is greater than (or equal to) this option will be included in the range. When `reverse=true` the order will be reversed, but the records streamed will be the same.
  * `'lt'` (less than), `'lte'` (less than or equal) define the higher bound of the range to be streamed. Only key/value pairs where the key is less than (or equal to) this option will be included in the range. When `reverse=true` the order will be reversed, but the records streamed will be the same.
  * `'start', 'end'` legacy ranges - instead use `'gte', 'lte'`
  * `'match'` *(string)*: use the minmatch to match the specified keys.
    * Note: It will affect the range[gt/gte or lt/lte(reverse)] options maybe.
  * `'limit'` *(number, default: `-1`)*: limit the number of results collected by this stream. This number represents a *maximum* number of results and may not be reached if you get to the end of the data first. A value of `-1` means there is no limit. When `reverse=true` the highest keys will be returned instead of the lowest keys.
  * `'reverse'` *(boolean, default: `false`)*: a boolean, set true and the stream output will be reversed. 
  * `'keys'` *(boolean, default: `true`)*: whether the `'data'` event should contain keys. If set to `true` and `'values'` set to `false` then `'data'` events will simply be keys, rather than objects with a `'key'` property.
  * `'values'` *(boolean, default: `true`)*: whether the `'data'` event should contain values. If set to `true` and `'keys'` set to `false` then `'data'` events will simply be values, rather than objects with a `'value'` property.


__return__

* object: the read stream object


the standard `'data'`, '`error'`, `'end'` and `'close'` events are emitted.
the `'last'` event will be emitted when the last data arrived, the argument is the last raw key(no decoded).
if no more data the last key is `undefined`.


### Examples


filter usage:

```js
db.createReadStream({filter: function(key, value){
    if (/^hit/.test(key))
        return db.FILTER_INCLUDED
    else key == 'endStream'
        return db.FILTER_STOPPED
    else
        return db.FILTER_EXCLUDED
}})
  .on('data', function (data) {
    console.log(data.key, '=', data.value)
  })
  .on('error', function (err) {
    console.log('Oh my!', err)
  })
  .on('close', function () {
    console.log('Stream closed')
  })
  .on('end', function () {
    console.log('Stream closed')
  })
```

next and last usage for paged data demo:

``` js

var callbackStream = require('callback-stream')

var lastKey = null;

function nextPage(db, aLastKey, aPageSize, cb) {
  var stream = db.readStream({next: aLastKey, limit: aPageSize})
  stream.on('last', function(aLastKey){
    lastKey = aLastKey;
  });

  stream.pipe(callbackStream(function(err, data){
    cb(data, lastKey)
  }))

}

var pageNo = 1;
dataCallback = function(data, lastKey) {
    console.log("page:", pageNo);
    console.log(data);
    ++pageNo;
    if (lastKey) {
      nextPage(db, lastKey, 10, dataCallback);
    }
    else
      console.log("no more data");
}
nextPage(db, lastKey, 10, dataCallback);
```


## Hooks

Hooks are specially built into Sublevel so that you can 
do all sorts of clever stuff, like generating views or
logs when records are inserted!

Records added via hooks will be atomically inserted with the triggering change.


### Subkey.pre/post()

1. subkey.pre(opType, function(opType, operation))
2. subkey.pre(opType, aKeyPattern, function(opType, operation))
3. subkey.pre(opType, aRangeObject, function(opType, operation))


operation: {path:path, key:key, value:value, triggerBefore: true, triggerAfter: true}

the opType could be `PUT_OP`, `DEL_OP` or `TRANS_OP`.

if hook proc can return 

* `HALT_OP` to halt this operation
* `CONTINUE_OP`: or return nothing to continue (defaults)

```js
consts   = require('nosql-subkey/lib/consts')
PUT_OP   = consts.PUT_OP
DEL_OP   = consts.DEL_OP
TRANS_OP = consts.TRANS_OP
HALT_OP  = consts.HALT_OP
```

if it is TRANS_OP the hook function arguments is:

function (opType, operation, add)

the `'add'` argument is a function to add a new operation to the transaction.



```js
//you should be careful of using the add() function
//maybe endless loop in it. u can disable the trigger
add({
    key:...,
    value:...,
    type:'put' or 'del',
    triggerBefore: false, //default is true. whether trigger this key on pre hook.
    triggerAfter: false   //default is true. whether trigger this key on post hook.
})
add(false): abandon this operation(remove it from the batch).
```

### Hooks Example

Whenever a record is inserted,
save an index to it by the time it was inserted.

``` js
var sub = root.subkey('SEQ')

root.pre(function (ch, add) {
  add({
    key: ''+Date.now(), 
    value: ch.key, 
    type: 'put',
    // NOTE: pass the destination db to add the value to that subsection!
    path: sub
  })
})

root.put('key', 'VALUE', function (err) {
  // read all the records inserted by the hook!
  sub.createReadStream().on('data', console.log)
})
```

Notice that the `parent` property to `add()` is set to `sub`, which tells the hook to save the new record in the `sub` section.

### Hooks Another Example

``` js
var sub = root.subkey('SEQ')

//Hooks range 
root.pre({gte:"", lte:"", path:""}, function (ch, add) {
  add({
    key: ''+Date.now(), 
    value: ch.key, 
    type: 'put',
    // NOTE: pass the destination db to add the value to that subsection!
    path: sub
  })
})

//hooks a key, and the key can be relative or absolute key path and minimatch supports.
root.pre("a*", function (ch, add) {
  //NOTE: add(false) means do not put this key into storage.
  add({
    key: ''+Date.now(), 
    value: ch.key, 
    type: 'put',
    // NOTE: pass the destination db to add the value to that subsection!
    path: sub
  })
})
```

## Batches

In `sublevel` batches also support a `prefix: subdb` property,
if set, this row will be inserted into that database section,
instead of the current section, similar to the `pre` hook above.

``` js
var sub1 = root.subkey('SUB_1')
var sub2 = root.subkey('SUB_2')

sub2.batch([
  {key: 'key', value: 'Value', type: 'put'},
  {key: 'key', value: 'Value', type: 'put', path: sub2},
  {key: '../SUB_1/key', value: 'Value', type: 'put', path: sub2},
], function (err) {...})
```

## Internal Storage Format for Key

The internal key path storage like file path, but the path separator can be customize.

+ supports subkey uses other separators, and you can change the default keys separator
  * the '%' can not be used as separator, it is the escape char.
  * the default subkey's separator is "#" if no any separator provided.
  * the others can have the subkeys too:
    * '/path/key/.attribute/#subkey'
    * optimalize performance for searching, use the new SUBKEY_SEPS design.
* customize usage:

``` js
    var codec = require('level-subkey/lib/codec')
    codec.SUBKEY_SEPS = ["/|-", "#.+"] //the first char is the default subkey separator, others are customize separator. 
    subkey.put("some", "value", {separator: '|'})
    //list all key/value on separator "|"
    subkey.createReadStream({separator: '.'})
    //it will return all prefixed "|" keys: {key: "|abc", value:....}
```

* the default SUBKEY_SEPS is ['/.!', '#\*&']

``` js
var stuff = root.subkey('stuff')
var animal = stuff.subkey('animal')
var plant = stuff.subkey('plant')

animal.put("pig", value, function () {})
// stored raw key is : "/stuff/animal#pig"
// decoded key is: "/stuff/animal/pig"
animal.put("../plant/cucumber", value, function (err) {})
// stored raw key is : "/stuff/plant#cucumber"
// decoded key is: "/stuff/animal/cucumber"
root.put("/stuff/animal/pig/.mouth", value, function(err){})
// stored raw key is : "/stuff/animal/pig*mouth"
// decoded key is: "/stuff/animal/pig/.mouth"
root.put("/stuff/animal/pig/.ear", value, function(err){})
// stored raw key is : "/stuff/animal/pig*ear"
// decoded key is: "/stuff/animal/pig/.ear"
root.put("/stuff/animal/pig/.ear/.type", value, function(err){})
// stored raw key is : "/stuff/animal/pig/.ear*type"
// decoded key is: "/stuff/animal/pig/.ear/.type"

```



## License

MIT



