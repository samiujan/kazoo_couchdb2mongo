# kazoo_couchdb2mongo
This is a simple script to convert json data fields exported from a Kazoo CouchDB instance into correct datetime and number formats and then import into a MongoDB instance

### Requirements:
- pymongo

Example usage:

```
./cleancdrs.py -if ../mydata.json -cs "mongodb://localhost:27017" -db mydatabase -col mycollection
```

### TODO:
- Load the json data in chunks
- Add tests
