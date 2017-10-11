
const { Store, Query, $, Patch, Operations, Client, IndexMap, DoesNotExist } = require( '../store');
const expect = require('chai').expect;
const debug = require('debug')('db-plumbing-mongo~tests');
const { MongoClient } = require('mongodb');
const Ops = Operations;

require('dotenv').config();

//const PATCH_OPTS = { map:true, key: e=>e.uid, value: e=>e, entry: (k,v)=>v }

function inlineDebug(msg) {  return a => { debug(msg, a); return a; }  }

class Simple { 
    constructor(uid, a, b, tags) { this.uid = uid; this.a=a; this.b=b; this.tags = tags} 
    static fromJSON({uid,a,b,tags}) { return new Simple(uid,a,b,tags); }
}

const byA = Query.from( { a : $.a } );
//const byTag = Query.from( { tags: { $has: $.tag } } );

const TEST_COLLECTION = 'testsimple';

function getStore() {
     return new Client(process.env.DATABASE_URL).getStore(TEST_COLLECTION, Simple);
}

describe('Store', () => {

    beforeEach((done) => {
        MongoClient.connect(process.env.DATABASE_URL)
            .then(db => db.dropCollection(TEST_COLLECTION))
            .then(() => done(), done)
    });


    it('creates and retrieves test object in store', (done) => {
        let store = getStore();
            store.update(new Simple(1,'hello','world'))
                .then(() => store.find(1))
                .then(inlineDebug('found'))
                .then(result=> {
                        expect(result.a).to.equal('hello');
                        expect(result.b).to.equal('world');
                    })
                .then(()=>done(), done);
            
    });

    it('creates updates and retrieves test object in store', (done) => {
        let store = getStore();
            store.update(new Simple(1,'hello','world'))
                .then(() => store.find(1))
                .then(inlineDebug('found'))
                .then(result=> {
                        expect(result.a).to.equal('hello');
                        expect(result.b).to.equal('world');
                    })
                .then(() => store.update(new Simple(1, 'cruel', 'world')))
                .then(() => store.find(1))
                .then(inlineDebug('found'))
                .then(result=> {
                        expect(result.a).to.equal('cruel');
                        expect(result.b).to.equal('world');
                    })
                .then(()=>done(), done);
    });


    it('creates multiple objects in store and finds by simple index', (done) => {

        let store = getStore();
            
        store.update(new Simple(1,'hello','world'))
                .then(() => store.update(new Simple(2, 'hello','friend')))
                .then(() => store.update(new Simple(3, 'goodbye', 'Mr. Chips')))
                .then(() => store.findAll(byA, {a:'hello'}).toArray())
                .then(result=> {
                        expect(result).to.have.length(2);
                        expect(result[0].b).to.equal('world');
                        expect(result[1].b).to.equal('friend');
                    })
                .then(()=>done(), done);
    });
/*
    it('creates multiple objects in store and finds by array field index', (done) => {

        let store = getStore();
            
        store.update(new Simple(1,'hello','world',['one', 'two']))
                .then(() => store.update(new Simple(2, 'hello','friend',['three','one'])))
                .then(() => store.update(new Simple(3, 'goodbye', 'Mr. Chips',['two','three'])))
                .then(() => store.findAll(byTag, 'three'))
                .then(result=> {
                        expect(result).to.have.length(2);
                        expect(result[0].uid).to.equal(2);
                        expect(result[1].uid).to.equal(3);
                    })
                .then(()=>done(), done);
    });
*/
    it('Creates multiple objects in store and deletes by index', (done) => {

        let store = getStore();
            
        store.update(new Simple(1,'hello','world'))
                .then(() => store.update(new Simple(2, 'hello','friend')))
                .then(() => store.update(new Simple(3, 'goodbye', 'Mr. Chips')))
                .then(() => store.removeAll(byA, {a:'hello'}))
                .then(() => store.all.toArray())
                .then(result => {
                        expect(result).to.have.length(1);
                        expect(result[0].a).to.equal('goodbye');
                        expect(result[0].b).to.equal('Mr. Chips');
                    })
                .then(()=>done(), done);
    });

    it('Find item that does not exist throws Store.DoesNotExist', (done) => {

        let store = getStore();
        
        store.update(new Simple(1,'hello','world'))
                .then( () => store.find(2))
                .then( () => chai.fail('call should not succeed') )
                .then( null, err => expect(err).to.be.instanceof(DoesNotExist) )
                .then( () => done(), done );
            }
    );

    it('Can do bulk update in store', (done) => {

            let store = getStore();

            store.update(new Simple(1,'hello','world'))
                .then(() => store.update(new Simple(2, 'hello','friend')))
                .then(() => store.update(new Simple(3, 'goodbye', 'Mr. Chips')))
                .then(() => store.bulk(new Ops.Map([ [1, new Ops.Mrg( { b: new Ops.Rpl('pizza') } ) ]])))
                .then(() => store.find(1))
                .then(inlineDebug('found'))
                .then(result=> {
                        expect(result.b).to.equal('pizza');
                    })
                .then(()=>done(), done);
            }
        );
    }
);
