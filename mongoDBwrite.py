from flask import Flask, jsonify,request
from pymongo import MongoClient
import json
app = Flask(__name__)
app.config["DEBUG"] = True   
#client = MongoClient("mongodb://127.0.0.1:27017")  # host uri
client = MongoClient("mongodb://localhost:27017")  # host uri
#val mongoClient: MongoClient = MongoClient("mongodb://localhost")


#db = client.mymongodb  # Select the database
db=client.shop

#tasks_collection = db.task  # Select the collection name
tasks_collection = db.corona16


initial_tasks = [task for task in tasks_collection.find()]
# print(len(initial_tasks))



@app.route('/api/tasks', methods=['GET'])
def get_tasks():
    all_tasks = tasks_collection.find({},{u'_id':0})        #usecase service class method-totalnooftweet  controller class(all api's),repositry class(query),service class(used methods of repositry class methods)
    task_list = []
    for task in all_tasks:
        task_list.append(task)
    #    task_list.append({'_id': task['_id'], 'tweet': task['tweet'], 'username': task['username'],'timestamp':task['timestamp'],'country':task['country']})
    
    #return jsonify({'tasks': task_list})
    return jsonify(task_list)

@app.route('/api/insert-document', methods=['POST'])
def insert_document():
    req_data = request.get_data()
    x=req_data.split(",")
    y = {
          "country":x[0],
          "day": x[2],
          "month": x[1],
          "tweet": x[3]
    }
    if(tasks_collection.insert(y)):
        return "inserted"
    else:
         return "fail"
