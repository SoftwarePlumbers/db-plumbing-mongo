
const { Store, Client, IndexMap } = require( '../store');
const Patch = require('typed-patch');
const expect = require('chai').expect;
const debug = require('debug')('db-plumbing-mongo~tests');
const { MongoClient } = require('mongodb');
const Ops = Patch.Operations;

//const PATCH_OPTS = { map:true, key: e=>e.uid, value: e=>e, entry: (k,v)=>v }

function withDebug(a) { debug(a); return a; }

class Simple { 
    constructor(uid, a, b) { this.uid = uid; this.a=a; this.b=b; } 
    static fromJSON({uid,a,b}) { return new Simple(uid,a,b); }
}

function byA(a, simple) { return simple.a == a; }

const INDEX_MAP = new IndexMap().addSimpleField(byA, "a");
const TEST_COLLECTION = 'testsimple';

const TEST_URL = 'mongodb://mongo1.softwareplumbers.net:27017/maximally-me';

function getStore() {
     return new Client(TEST_URL).getStore(TEST_COLLECTION, Simple, INDEX_MAP);
}


describe('Store', () => {

    beforeEach(() => {
        MongoClient.connect(TEST_URL)
            .then(db => db.dropCollection(TEST_COLLECTION));
    });


    it('creates and retrieves test object in store', (done) => {
        let store = getStore();
            store.update(new Simple(1,'hello','world'))
                .then(() => store.find(1))
                .then(withDebug)
                .then(result=> {
                        expect(result.a).to.equal('hello');
                        expect(result.b).to.equal('world');
                    })
                .then(()=>done(), done);
            
    });

    it('creates multiple objects in store and finds by index', (done) => {

        let store = getStore();
            
        store.update(new Simple(1,'hello','world'))
                .then(() => store.update(new Simple(2, 'hello','friend')))
                .then(() => store.update(new Simple(3, 'goodbye', 'Mr. Chips')))
                .then(() => store.findAll(byA, 'hello'))
                .then(result=> {
                        expect(result).to.have.length(2);
                        expect(result[0].b).to.equal('world');
                        expect(result[1].b).to.equal('friend');
                    })
                .then(()=>done(), done);
    });

    it('Find item that does not exist throws Store.DoesNotExist', (done) => {

        let store = getStore();
        
        store.update(new Simple(1,'hello','world'))
                .then( () => store.find(2))
                .then( () => chai.fail('call should not succeed') )
                .then( null, err => expect(err).to.be.instanceof(Store.DoesNotExist) )
                .then( () => done(), done );
            }
    );

    it('can do bulk update in store', (done) => {

            let store = getStore();

            store.update(new Simple(1,'hello','world'))
                .then(() => store.update(new Simple(2, 'hello','friend')))
                .then(() => store.update(new Simple(3, 'goodbye', 'Mr. Chips')))
                .then(() => store.bulk(new Ops.Map([ [1, new Ops.Mrg( { b: new Ops.Rpl('pizza') } ) ]])))
                .then(() => store.find(1))
                .then(withDebug)
                .then(result=> {
                        expect(result.b).to.equal('pizza');
                    })
                .then(()=>done(), done);
            }
        );
    }
);
