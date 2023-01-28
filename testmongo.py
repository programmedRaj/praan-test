import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
# mydb = myclient["testdb"]
mydb = myclient["testpraan"]
# mycol = mydb["customers"]
mycol = mydb["praandata"]

myresult = mycol.find()
for x in myresult:
  print(x)
  
# mydict = { "name": "Johnx", "address": "Highway 37" }

# x = mycol.insert_one(mydict)