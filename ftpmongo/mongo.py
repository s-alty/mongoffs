import json

import bson.json_util
import pymongo

DB_HOST = '127.0.0.1'
DB_PORT = 27017

AUTHENTICATION_TYPE = 'SCRAM-SHA-256'


def authenticate(username, password):
    client = pymongo.MongoClient(
        DB_HOST,
        DB_PORT,
        username=username,
        password=password,
        authMechanism=AUTHENTICATION_TYPE
    )
    client.test.command({'ping': 1})
    return client


def list_databases(client):
    return client.list_database_names()

def list_collections(client, db):
    return getattr(client, db).list_collection_names()

def list_documents(client, db, collection):
    db = getattr(client, db)
    coll = getattr(db, collection)
    map_function = """
    function(){
       emit(this._id, Object.bsonsize(this));
    }
    """
    reduce_function = """
    function(key, values){
      return values[0];
    }
    """
    return coll.map_reduce(map_function, reduce_function, {'inline': 1})['results']

def get_file_or_document(client, db, collection, _id):
    # TODO: could document be None?
    db = getattr(client, db)
    coll = getattr(db, collection)
    doc = coll.find_one({'_id': _id})

    # returns bytes
    if '_bindata' in doc:
        return doc['_bindata']
    return json.dumps(doc, default=bson.json_util.default).encode('ascii')


def store_file_or_document(client, db, collection, _id, contents):
    # contents is either json or raw binary data
    # if it's json just insert that directly
    # if its generic binary data then wrap it in a document
    try:
        document = json.loads(contents, object_hook=bson.json_util.object_hook)
    except json.JSONDecodeError:
        document = {'_bindata': contents}

    document['_id'] = _id

    db = getattr(client, db)
    coll = getattr(db, collection)
    coll.replace_one({'_id': _id}, document, upsert=True)

def create_collection(client, db, collection):
    db = getattr(client, db)
    try:
        db.create_collection(collection)
    except pymongo.errors.CollectionInvalid:
        # collection already existed
        pass
