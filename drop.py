from pymongo import MongoClient

client = MongoClient(port=27017)
db = client["lab4"]

db.zno_RESULTS_19_20.drop()
db.buffer_table.drop()
