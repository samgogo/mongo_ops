#! /usr/bin/env python3
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
collection = 'passerby'

def time_keep(numMonthes):
    now = int(time.time())    # current time
    oneMonth = 60 * 60 * 24 * 30    # timestamps in one month
    t = now - numMonthes * oneMonth    # remove the data before this time
    return t

def get_collection():
    client = MongoClient(uri)
    db = client[dbs]
    collection = db[collection]
    return collection

collection =  get_collection
timeKeep = time_keep(numMonthes)
count = collection.count_documents({'create_time': {"$lt": timeKeep}})

print("start cleaning collection, %d documents in %d monthes will be removed in collection %s" % (count, numMonthes, collection.name))

result = collection.find({'create_time': {"$lt": timeKeep}})
for r in result:
   if 'back_pic_src' in r:
       pprint.pprint(r['back_pic_src'])
   else:
       print('not exist')
#  print("%d documents removed in %s" % (result, collection.name))
