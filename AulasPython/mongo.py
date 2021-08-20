import connections

# client = MongoClient('ffborelli.ddns.net', 27017)
# db = client.big_data
# collection = db.tacilio_pereira

client = connections.getConnMongo()
db = client.big_data
collection = db.tacilio_pereira

# print(client)


# all = collection.find({"id" : 6 })
# for obj in all:
#     print(all)

# one = collection.find_one({ "id" : 1})
# print(one)

insert_obj = collection.insert( { "id": 26, "name": "Pereira"} )
print(insert_obj)

# objs = ( [ {"id": 7, "name": "Name 2"}, { "id": 8, "name": "Name 3 "} ] ) 

# insert_obj = collection.insert( objs )

# print (insert_obj)

# updates = collection.update( 
#     {"id": 6},
#     {
#         '$set': {
#             "name": "Name 20"
#         }
#     }
#  )
# print(updates)

#deletar = collection.delete_one ( { "id":8 } )

