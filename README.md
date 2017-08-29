# ![Software Plumbers](http://docs.softwareplumbers.com/common/img/SquareIdent-160.png) DB Plumbing (Map)

Mongo database wrapper compatible with db-plumbing-map and db-plumbing-rest.

## Tl;DR

```javascript
let store = new Client("mongodb://mongo1.net:27017/test").getStore("mycollection");

store.update({ uid: 1, a: "hello", b: "sailor"});

value=store.find(1);
```

and value should be `{uid:1, a:"hello", b:"sailor"}`

The store supports remove, find by criteria, and remove by criteria operations. It also supports a bulked update operation based on the [typed-patch](https://npmjs.org/packages/typed-patch) library.

This implementation should be interoperable with db-plumbing-map or db-plumbing-rest.

For the latest API documentation see [The Software Plumbers Site](http://docs.softwareplumbers.com/db-plumbing-mongo/master)



