from flask import jsonify
from src.jsonEncoder import JSONEncoder
from src.repository import Repository
from nltk.corpus import stopwords
class Service:
    def __init__(self,addr):
        self.rp = Repository(addr)
    def task1(self,country):
        all_tasks1 = self.rp.t1(country)
        val1=list(all_tasks1)
        return JSONEncoder().encode(val1)
        # return jsonify(val)

    def task1_spark(self,country):
        all_tasks1_spark = self.rp.t1_spark(country)
        return jsonify(all_tasks1_spark)

    def task2(self,country):
        all_tasks2 = self.rp.t2(country)
        val2=list(all_tasks2)
        return JSONEncoder().encode(val2)

    def task2_spark(self,country):
        all_tasks2_spark = self.rp.t2_spark(country)
        return jsonify(all_tasks2_spark)

    def task3(self,limit):
        all_task3=self.rp.t3(limit)
        val3=list(all_task3)
        return jsonify(val3)

    def task3_spark(self,limit):
        all_task3_spark=self.rp.t3_spark(limit)
        #return jsonify(all_task3_spark)
        #return  jsonify(all_task3_spark)
        return JSONEncoder().encode(all_task3_spark)

    def task4(self,limit,country):
        all_task4=self.rp.t4(limit,country)
        val4=list(all_task4)
        return JSONEncoder().encode(val4)

    def task4_spark(self,limit,country):
        all_task4_spark=self.rp.t4_spark(limit,country)
        #return jsonify(all_task4_spark)
        return JSONEncoder().encode(all_task4_spark)
