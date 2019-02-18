from pymongo import MongoClient
import time
import pprint

host = '192.168.18.61'
port = '27017'
user = ''
password = ''
if user.strip() == "":
    uri = "mongodb://%s:%s" % (host, port)
else:
    uri = "mongodb://%s:%s@%s:%s" % (user, password, host, port)
numMonthes = 8    # how many months to keep

dbs = 'face'

collections = ['passerby',]


def timeKeep(numMonthes):
    now = int(time.time())    # current time
    oneMonth = 60 * 60 * 24 * 30    # timestamps in one month
    t = now - numMonthes * oneMonth    # remove the data before this time
    return t


def get_db(dbs):
    client = MongoClient(uri)
    db = client[dbs]
    return db


timeKeep = timeKeep(numMonthes)   # timestamp

print("timestamp:", timeKeep)

db = get_db(dbs)

for collection in collections:
    collection = db[collection]
    print("start cleaning collection %s" % (collection.name))
    count = collection.count_documents({'create_time': {"$lt": timeKeep}})
    print(count)
    result = collection.find({'create_time': {"$lt": timeKeep}})
    pprint.pprint(result[4])
#    print("%d documents removed in %s" % (result, collection.name))

