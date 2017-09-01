/** @module db-plumbing-mongo
 *
 */
'use strict';

const { MongoClient } = require('mongodb');
const { DoesNotExist } = require('db-plumbing-map');
const { Operations } = require('typed-patch');

const debug = require('debug')('db-plumbing-mongo');


/** Metadata about indexes.
 *
 * The generic store interface supports a findAll(index, value) operation that returns a dataset dependent on index
 * and value. Index is, by convention, an named function (value, item) => boolean and the result of store.findAll(index,value)
 * should be equal to store.all().filter(item => index(value, item)).
 *
 * An IndexMap maps the named fuction above to a different function which instead takes a value and outputs an appropriate
 * mongodb query. The implementations of findAll and removeAll use this mongodb query to identify records, instead of 
 * performing a linear search.
 *
 * Currently only simple single-field indexes are supported by IndexMap. The intention is to support compound indexes
 * also.
 */
class IndexMap {
    
    constructor() {
        this.maps = {};
    }

    /** Tell the Mongo client about a simple index.
     *
     * @param index {Function} a named function (value,item)=>boolean that filters items in a store
     * @param name {String} the name of a field in mongodb for which item.field == value is equivalent to index
     * @returns this Indexmap (for fuent construction)
     */
    addSimpleField(index, name) {
        this.maps[index.name] = value => Object.defineProperty({}, name, { value: value, enumerable: true, writable: false });
        return this;
    }

    /** Convert an index and value to mongodb criteria 
    *
    * @param index {Function} an index filter previously added to this map
    * @value value to filter on
    * @returns a monogodb query that will filter items identically to the supplied filter function
    */
    toMongoCriteria(index, value) {
        let map = this.maps[index.name];
        if (map === undefined) throw new Error('mongo does not understand index ' + index.name);
        return map(value);
    }
}

const DEFAULT_OPTIONS = {
    key: value => value.uid,    // How to find a key from a value
    value: entry => entry,      // How to map an entry to a value
    entry: (_id, data) => Object.assign({}, data, {_id}) // how to make a key/value pair to an entry
};

/** MongoDB document store.
 *
 * Wraps the mongo interface in a simple but powerful interface. As well as the standard CRUD ops, a Store
 * supports a bulk operation that takes a typed-patch Map operation. This can be used to efficiently express
 * a range of complex update operations, and can be used to sync one store with data held in another store.
 */
class Store {

    static get DoesNotExist() { return DoesNotExist; }

    /** Recursively concert a typed-patch Mrg operation into a mongo updaate instructions
     *
     * TODO: make this work for array elements.
     */
    static _diffToMongo(diff, mongo, context) {
        for (let name in diff) {
            let property = diff[name];
            if (property instanceof Operations.Rpl) 
                mongo.$set[context + name] = property.data;
            else if (property === Operations.DEL) 
                mongo.$unset[context + name] = '';
            else if (property instanceof Operations.Mrg) 
                Store.diffToMongo(diff[property].data, mongo, context + property + '.');
        }
        return mongo;
    }

    static diffToMongo(diff) {
        let mongo = Store._diffToMongo(diff, { $set: {}, $unset: {}}, '');
        if (Object.getOwnPropertyNames(mongo.$set).length === 0) delete mongo.$set;
        if (Object.getOwnPropertyNames(mongo.$unset).length === 0) delete mongo.$unset;
        return mongo;
    }

    /** Constructor.
    * 
    * Create a store that records objects of the given type in the supplied mongodb collection.
    *
    * @param collection A mongodb collection or a promise thereof
    * @param type {Function} a constructor (perhaps a base class constructor) for elements in this store.
    * @param indexes {IndexMap} an IndexMap object that tells mongo how to handle searches on non-ids.
    */ 
    constructor(collection, type = Object, indexes = new IndexMap(),  options = DEFAULT_OPTIONS) {

        console.assert(collection && typeof collection === 'object', 'collection must be an object');
        console.assert(type && typeof type === 'function','type must be a constructor');
        console.assert(indexes && typeof indexes === 'object' && indexes instanceof IndexMap, 'indexes must be an instance of IndexMap');

        this.collection = Promise.resolve(collection); // It doesn't matter if collection wasn't a promise - it is now.
        this.type = type;
        this.indexes = indexes;
        this.options = options;
    }

    /** Find an object by its unique key
    *
    * @param _id Unique object identifier
    * @returns A promise that either resolves with the object stored for the given key, or rejects with a DoesNotExist error.
    */
    find(_id) { 
        debug('find', _id);
        return this.collection
            .then(collection => collection.findOne({_id}))
            .then(item => { 
                if (item === null) throw new DoesNotExist(_id); 
                return this.type.fromJSON(this.options.value(item)); 
            });
    }

    /** Get all objects in the store
    *
    * @returns a promise that resolves to an array contianing all values in the store
    */
    get all() {
        debug('all');
        return this.collection
            .then(collection => collection.find().toArray())
            .then(items => items.map(item => this.type.fromJSON(this.options.value(item))));
    }

    /** Find objects by index
    *
    * Index is, by convention, an named function (value, item) => boolean and the result of store.findAll(index,value)
    * should be equivalent to store.all().filter(item => index(value, item)). However, other implementations of store
    * may optimize this algorithm to use a better algorithm than a simple linear search. The distinct 'findAll' method
    * allows for that.
    *
    * @param index {Function} A function that takes a value and a stored object and returns true or false
    * @returns A promise of an array containing all elements for which the function returns true for the given value
    */ 
    findAll(index, value)  { 
        debug('findAll', index, value);
        let mongo_criteria = this.indexes.toMongoCriteria(index,value);
        return this.collection
            .then(collection => collection.find(mongo_criteria).toArray())
            .then(items => items.map(item => this.type.fromJSON(this.options.value(item))));
    }

    /** Update or add an object in the store.
    *
    * @param object to add or update.
    * @returns a resolved promise.
    */
    update(object) { 
        debug('Update',object);
        let _id = this.options.key(object);
        return this.collection
            .then(collection => collection.updateOne(
                { _id }, 
                this.options.entry(_id,object), 
                {w : 1, upsert: true}));
            
    }

    /** Remove an object from the store.
    *
    * @param key unique identifier of object to remove.
    * @returns a promise that resolves to true if the object is removed, false otherwise.
    */
    remove(_id)  { 
        return this.collection
            .then(collection => collection.deleteOne({_id}, {w: 1}))
            .then(result => { 
                if (result.deletedCount != 1) debug('bad delete count', result.deletedCount); 
                return result; 
            });
    }

    /** Remove multiple objects from the store
    *
    * @param index {Function} a function that takes a value and an object from the store and returns true or false
    * @param a value that determines which objects are removed.
    */ 
    removeAll(index, value) {
        return this.collection
            .then(collection => collection.remove(this.indexes.toMongoCriteria(index,value), {w: 1}));
    }

    /** Internal function that updates an individual record using a typed-patch Mrg operation */
    _updateFromDiff(_id, diff) {
        const mongoQuery = Store.diffToMongo(diff.data);
        debug('_updateFromDiff', _id, mongoQuery);
        return this.collection
            .then(collection => collection.updateOne({ _id }, mongoQuery, {w : 1}));    
    }

    /** Internal function that removes documents with the supplied ids from the collection */
    _bulkRemove(ids) {
        return this.collection
            .then(collection => collection.deleteMany({ _id: { $in: ids }}, {w : 1}));    
    }

    /** Internal function that inserts each item in the supplied array into the collection */
    _bulkInsert(items) {
        return this.collection
            .then(collection => collection.insertMany(items, {w : 1}));    
    }

    /** Execute multiple update operations on the store
    *
    * @param patch { Patch.Operation } Information to update in patch format. 
    * @see [Typed Patch](https://www.npmjs.com/package/typed-patch)
    */
    bulk(patch) {
        console.assert(patch instanceof Operations.Map, 'Store only understands map diffs');
        debug('MongoClient - bulk', patch);
        let deletes = [];
        let inserts = [];
        let updates = Promise.resolve();
        for (let [key,op] of patch.data) {
            debug('update', key, op);
            // TODO: Handle Rpl?
            if (op instanceof Operations.Mrg) {
                updates = updates.then( () => this._updateFromDiff(key, op) );
            } else if (op instanceof Operations.Ins) {
                debug('queueing insert', op);
                inserts.push(this.type.fromJSON(op.data));
            } else if (op === Operations.DEL) {
                debug('queueing delete', op);
                deletes.push(key);
            } else {
                throw new Error('Unkown diff operation ' + op);
            }
        }
       
        let operation = updates;
        if (inserts.length > 0) operation = operation.then( () => this._bulkInsert(inserts) );
        if (deletes.length > 0) operation = operation.then( () => this._bulkRemove(deletes) ); 

        return operation;
    }
}

/** Encapsulates a mongodb database connection.
 */
class Client {

    /** Constuctor
     *
     * @param url {String} URL for the mongodb database (including port and db name)
     */
    constructor(url) {
        this.db = MongoClient.connect(url);
    }

    /** Get a store representing the given collection that returns objects of the given type.
     *
     * @param collection {String} mongodb collection name
     * @param type {Function} constructor for contained objects (defaults to Object)
     * @param indexes {IndexMap} indexes (defaults to an empty IndexMap)
     * @param options Overide default options.z
     */
    getStore(collection, type, indexes, options) {
        return new Store(this.db.then(db => db.collection(collection)), type, indexes, options);
    }

}


/** the public API of this module. */
module.exports = { Store, Client, IndexMap, DoesNotExist };

