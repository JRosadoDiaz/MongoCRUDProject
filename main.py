import os
import pandas
import pymongo
import json

from pymongo.read_preferences import Secondary

client = pymongo.MongoClient('mongodb://localhost:27017')#("localhost", 27017)
db = client.test # Name of database
people = db.people # Name of collection

def main():
    #print(import_people('dataCsv.csv'))
    #read({"_id":"1"})
    #print(create_person("12345","jose","rosado", 2000))
    update({"_id":"12345"}, {"first_name":"jeffrey"})
    
    #delete({"_id":"12345"})
    print(db_count())

def import_people(csv_path):
    print('importing csv')
    data = pandas.read_csv(csv_path)
    toLoad = json.loads(data.to_json(orient='records'))
    people.remove()
    people.insert(toLoad)
    return 'records imported: ' + str(people.count())

def create_person(this_id, first_name, last_name, hire_year):
    result = people.insert_one({'_id': this_id, 'first_name': first_name, 'last_name': last_name, 'hire_year': hire_year})
    return result.inserted_id

def read(query): #(first, second)
    results = people.find(query) # (first,second)
    for item in results:
        print(item)

def db_count():
    return people.count_documents({})

def update(object_to_update, updated_info):
    people.update_one(object_to_update, {'$set': updated_info})
    read(object_to_update)#, {})

def delete(object_to_delete):
    people.delete_one(object_to_delete)
    read(object_to_delete)#, {})

if __name__ == "__main__":
    main()