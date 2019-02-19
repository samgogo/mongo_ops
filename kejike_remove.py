#! /usr/bin/env python3
from pymongo import MongoClient
import time, os, sys
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
collections = 'passerby_copy'

files_to_be_removed = []

root = '/mnt/mfs/projectCAM/face'

def time_keep(numMonthes):
    now = int(time.time())    # current time
    oneMonth = 60 * 60 * 24 * 30    # timestamps in one month
    t = now - numMonthes * oneMonth    # remove the data before this time
    return t

def get_collection():
    client = MongoClient(uri)
    db = client[dbs]
    collection = db[collections]
    return collection

collection = get_collection()
timeKeep = time_keep(numMonthes)
count = collection.count_documents({'create_time': {"$lt": timeKeep}})
print("start cleaning collection, %d documents in %d monthes will be removed in collection %s" % (count, numMonthes, collection.name))

docs_to_be_removed = collection.find({'create_time': {"$lt": timeKeep}})

pics_to_be_removed = ['back_pic_src', 'face_pic_src', 'show_pic_src']

for doc in docs_to_be_removed:
    for pic in pics_to_be_removed:
        if pic in doc:
            pic_path = root + '/' + doc[pic]
            if os.path.exists(pic_path):
                files_to_be_removed += [pic_path]
            else:
                print("%s was not found" % pic_path)

        else:
            print('was not found')
pprint.pprint(files_to_be_removed)
#    collection.delete_one(doc)

#  print("%d documents removed in %s" % (result, collection.name))


#try:
    # os.remove(pic_path)
#    print("%s was removed" % pic_path)
#except IOError:
#    print("Error: can\'t find file or read data")
