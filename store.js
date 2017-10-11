/** @module db-plumbing-mongo
 *
 */
'use strict';

const { MongoClient } = require('mongodb');
const { DoesNotExist } = require('db-plumbing-map');
const { Patch, Operations } = require('typed-patch');
const { AsyncStream } = require('iterator-plumbing');
const { Query, $ } = require('abstract-query');
const { MONGO } = require('mongo-query-format');

const debug = require('debug')('db-plumbing-mongo');

function withDebug(msg, val) { debug(msg, val); return val }


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

    static streamFromCursor(promise) {
        return new AsyncStream({
            next: ()=>promise.then(cursor=>cursor.next().then(value => ({ done: value===null, value }))) 
        });
    }

    /** Constructor.
    * 
    * Create a store that records objects of the given type in the supplied mongodb collection.
    *
    * @param collection A mongodb collection or a promise thereof
    * @param type {Function} a constructor (perhaps a base class constructor) for elements in this store.
    * @param [options = DEFAULT_OPTIONS] options
    */ 
    constructor(collection, type = Object, options = DEFAULT_OPTIONS) {

        console.assert(collection && typeof collection === 'object', 'collection must be an object');
        console.assert(type && typeof type === 'function','type must be a constructor');

        this.collection = Promise.resolve(collection); // It doesn't matter if collection wasn't a promise - it is now.
        this.type = type;
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
        return Store.streamFromCursor(this.collection.then(coll=>coll.find()))
            .map(item => this.type.fromJSON(this.options.value(item)));
    }

    /** Find objects by query
    *
    * @see [Abstract Query](https://www.npmjs.com/package/abstract-query)
    *
    * @param query {Query} a query object
    * @param [parameters] {Object} parameters for the query
    * @returns An async stream containing all elements for which the query predicate returns true for the given parameters
    */ 
    findAll(query, parameters = {})  { 
        debug('findAll', query, parameters);
        let mongo_criteria = query.bind(parameters).toExpression(MONGO);
        return Store.streamFromCursor(this.collection.then(coll=>coll.find(mongo_criteria)))
            .map(item => this.type.fromJSON(this.options.value(item)));
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

    /** Remove multiple objects from the store.
    *
    * @see [Abstract Query](https://www.npmjs.com/package/abstract-query)
    *
    * @param query {Query} a Query 
    * @param [parameters] {Object} optional parameters for query
    */ 
    removeAll(query, parameters = {}) {
        let mongo_criteria = query.bind(parameters).toExpression(MONGO);
        return this.collection
            .then(collection => collection.remove(mongo_criteria, {w: 1}));
    }

    /** Internal function that updates an individual record using a typed-patch Mrg operation
    * @private 
    */
    _updateFromDiff(_id, diff) {
        const mongoQuery = Store.diffToMongo(diff.data);
        debug('_updateFromDiff', _id, mongoQuery);
        return this.collection
            .then(collection => collection.updateOne({ _id }, mongoQuery, {w : 1}));    
    }

    /** Internal function that removes documents with the supplied ids from the collection 
    * @private 
    */
    _bulkRemove(ids) {
        return this.collection
            .then(collection => collection.deleteMany({ _id: { $in: ids }}, {w : 1}));    
    }

    /** Internal function that inserts each item in the supplied array into the collection 
    * @private 
    */
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
module.exports = { Store, Query, $, Patch, Operations, Client, DoesNotExist };

