import bson
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

def get_document(client, db, collection, _id):
    db = getattr(client, db)
    coll = getattr(db, collection)
    doc = coll.find_one({'_id': _id})
    return bson.BSON.encode(doc)
