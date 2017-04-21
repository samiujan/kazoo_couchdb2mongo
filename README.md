# kazoo_couchdb2mongo
This is a simple script to convert json data fields exported from a Kazoo CouchDB instance into correct datetime and number formats and then import into a MongoDB instance

### Requirements:
- python and pymongo

Example usage:

```
./cleancdrs.py -if ../mydata.json -cs "mongodb://localhost:27017" -db mydatabase -col mycollection
```

### Installation:

- Install Python 2.7
```
#ubuntu
sudo apt-get install python
```

- (Ideally) Install and activate virtualenv
```
#ubuntu
sudo apt-get install virtualenv
virtualenv venv
source venv/bin/activate
```
- Install pymongo
```
pip install pymongo
```
- 

### TODO:
- Load the json data in chunks
- Add tests
