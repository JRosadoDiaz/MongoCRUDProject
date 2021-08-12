import os
import pandas
import pymongo
import json

client = pymongo.MongoClient("localhost", 27017)
db = client.projectdb
people = db.people

def main():
    import_people('path here')

def import_people(csv_path):
    data = pandas.read_csv(csv_path)
    toLoad = json.loads(data.to_json(orient='records'))
    people.remove()
    people.insert(toLoad)
    return people.count()

def create_person(this_id, first_name, last_name, hire_year):
    result = people.insert_one({'_id': this_id, 'first_name': first_name, 'last_name': last_name, 'hire_year': hire_year})
    return result.inserted_ids

def read(first, second):
    results = people.find(first, second)
    for item in results:
        print(item)

def update(object_to_update, updated_info):
    people.update_one(object_to_update, {'$set': updated_info})
    read(object_to_update, {})

def delete(object_to_delete):
    people.delete_one(object_to_delete)
    read(object_to_delete, {})