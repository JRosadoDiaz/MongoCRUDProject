import os
import pandas
import pymongo
import json

from pymongo.read_preferences import Secondary

client = pymongo.MongoClient('mongodb://localhost:27017')#("localhost", 27017)
db = client.test # Name of database
people = db.people # Name of collection

def main():
    print('Create: ' + create_person('12345', 'CHRIS', 'CALABRESE', '2002'))
    print('Read: ')
    read({'_id': '12345'})
    print('Update: ')
    update({'_id': '12345'}, {'first_name': 'CHRIS2'})
    print('Delete: ')
    print(delete({'_id': '12345'}))

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

def read(query):
    results = people.find(query)
    for item in results:
        print(item)

def read_specified_values(first, second):
    results = people.find(first, second)
    for item in results:
        print(item)

def db_count():
    return people.count_documents({})

def update(object_to_update, updated_info):
    people.update_one(object_to_update, {'$set': updated_info})
    return read(object_to_update)

def delete(object_to_delete):
    people.delete_one(object_to_delete)
    return read(object_to_delete)

if __name__ == "__main__":
    main()