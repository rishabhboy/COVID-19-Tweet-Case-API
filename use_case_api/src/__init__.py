from flask import Flask,request
from nltk.corpus import stopwords
from src.service import Service

app=Flask(__name__)
app.config["DEBUG"] = True
#stop_words = set(stopwords.words('english'))

address = "mongodb://localhost:27017"
obj = Service(address)

@app.route('/api/q1', methods=['GET'])
def get_task1():
    if 'country' in request.args:
        country=str(request.args['country'])
    else:
        country="all"
    return obj.task1(country)

@app.route('/api/q1_spark', methods=['GET'])
def get_task1_spark():
    if 'country' in request.args:
        country=str(request.args['country'])
    else:
        country="all"
    return obj.task1_spark(country)

@app.route('/api/q2', methods=['GET'])
def get_task2():
    if 'country' in request.args:
        country=str(request.args['country'])
    else:
        country="all"
    if(country=="all"):
        return obj.task2(country)
    else:
        return obj.task2_spark(country)

@app.route('/api/q2_spark', methods=['GET'])
def get_task2_spark():
    if 'country' in request.args:
        country=str(request.args['country'])
    else:
        country="all"
    #return obj.task2_spark(country)
    if(country=="all"):
        return obj.task2(country)
    else:
        return obj.task2_spark(country)


@app.route('/api/q3', methods=['GET'])
def get_task3():
    if 'limit' in request.args:
        limit=int(request.args['limit'])
    else:
        limit=100
    return obj.task3(limit)

@app.route('/api/q3_spark',methods=['GET'])
def get_task3_spark():
    if 'limit' in request.args:
        limit=int(request.args['limit'])
    else:
        limit=100
    return  obj.task3_spark(limit)

@app.route('/api/q4',methods=['GET'])
def get_task4():
    if 'country' in request.args:
        country=str(request.args['country'])
    else:
        country="US"
    if 'limit' in request.args:
        limit=int(request.args['limit'])
    else:
        limit=100
    return  obj.task4(limit,country)

@app.route('/api/q4_spark',methods=['GET'])
def get_task4_spark():
    if 'country' in request.args:
        country=str(request.args['country'])
    else:
        country="US"
    if 'limit' in request.args:
        limit=int(request.args['limit'])
    else:
        limit=100
    return  obj.task4_spark(limit,country)


