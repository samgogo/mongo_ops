#! /usr/bin/env python3
from pymongo import MongoClient
import time, os, datetime
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
files_not_found = []
docs_to_be_removed_list = []
root = '/mnt/mfs/projectCAM/face'

def time_keep(numMonthes):
    now = int(time.time())    # current time
    oneMonth = 60 * 60 * 24 * 30    # timestamps in one month
    t = now - numMonthes * oneMonth
    return t

def get_collection():
    client = MongoClient(uri)
    db = client[dbs]
    collection = db[collections]
    return collection

# remove the data before this time
timeKeep = time_keep(numMonthes)          # timestamp
dateArray = datetime.datetime.fromtimestamp(timeKeep)
otherStyleTime = dateArray.strftime("%Y-%m-%d %H:%M:%S") # datetime

# get the collection
collection = get_collection()
count = collection.count_documents({'create_time': {"$lt": timeKeep}})

docs_to_be_removed = collection.find({'create_time': {"$lt": timeKeep}})
pics_to_be_removed = ['back_pic_src', 'face_pic_src', 'show_pic_src']

print("start cleaning collection...")
print("%d documents earlier than %s will be removed in collection %s" % (count, otherStyleTime, collection.name))

# gather files' paths
for doc in docs_to_be_removed:
    docs_to_be_removed_list += [doc]
    for pic in pics_to_be_removed:
        if pic in doc:
            pic_path = root + '/' + doc[pic]
            if os.path.exists(pic_path):
                files_to_be_removed += [pic_path]
            else:
                files_not_found += [pic_path]
        else:
            print('was not found')

# remove files
files_removed = 0
for file in files_to_be_removed:
    try:
        os.remove(pic_path)
        files_removed += 1
        print("%s was deleted. %d in %d" % (pic_path, files_removed, files_to_be_removed))
    except IOError:
        print("Error: can\'t find file or read data")
print("%d files was deleted" % files_removed)

# remove documents
docs_removed = 0
for doc in docs_to_be_removed_list:
    collection.delete_one(doc)
    docs_removed += 1
    print("documents is cleaning, %d in %d" % (docs_removed, count))
print("------------------------------------------------------------------------------------------------------------")
print("%d documents earlier than %s has been removed in collection %s" % (docs_removed, otherStyleTime, collection.name))
print("%d files was deleted, %d files was not found" % (files_removed, files_not_found.__len__()))
