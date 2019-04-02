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

numMonthes = 7  # how many months to keep
dbs = 'face'
collections = 'passerby'
pics_to_be_removed = ['back_pic_src', 'face_pic_src', 'show_pic_src']

files_to_be_removed = []
docs_to_be_removed_id = []
root = '/mnt/mfs/projectCAM/face'


def time_keep(numMonthes):
    now = int(time.time())  # current time
    oneMonth = 60 * 60 * 24 * 30  # timestamps in one month
    t = now - numMonthes * oneMonth
    return t


def get_collection():
    client = MongoClient(uri)
    db = client[dbs]
    collection = db[collections]
    return collection

# remove the data before this time
timeKeep = time_keep(numMonthes)  # timestamp
dateArray = datetime.datetime.fromtimestamp(timeKeep)
otherStyleTime = dateArray.strftime("%Y-%m-%d %H:%M:%S")  # datetime

# get the collection
collection = get_collection()
count = collection.count_documents({'create_time': {"$lt": timeKeep}})

docs_to_be_removed = collection.find({'create_time': {"$lt": timeKeep}})

# remove file
files_removed = 0
files_not_fount = 0
docs_removed = 0
for doc in docs_to_be_removed:
    for pic in pics_to_be_removed:
        pic_path = root + '/' + doc[pic]
        try:
            os.remove(pic_path)
            files_removed += 1
#            print("%s was deleted. %d in %d" % (pic_path, files_removed, count * 3))
        except IOError:
            files_not_fount += 1
    collection.delete_one(doc)
    docs_removed += 1
#    print("One document was deleted. %d in %d" % (docs_removed, count))

print("------------------------------------------------------------------------------------------------------------\n")
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
print("%d documents earlier than %s has been removed in collection %s.\n" % (docs_removed, otherStyleTime, collection.name))
print("%d files was deleted, %d files was not found.\n" % (files_removed, files_not_fount))
