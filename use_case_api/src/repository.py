from bson import SON
from flask import Response,jsonify
from pymongo import MongoClient
from nltk.corpus import stopwords
from pyspark.sql.functions import col
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split
import json
class Repository:
    def __init__(self, addr):
        self.client = MongoClient(addr)
        self.db = self.client.shop
        self.coll = self.db.corona16

        self.stop_words=["needn't", 'were', 'ain', 'don', 'did', 'or', 'they', 'will', "she's", "aren't", 'an', 'd', 'weren', 'won', "you'll", 'their', 'am', 'having', 'is', 'so', 'once', 'we', "shan't", 'and','Corona','Virus', 'again', 'during', 'other', 'doing', 'now', 'y', 'up', "wasn't", 'themselves', 't', 'few', 'some', 'against', 'here', 'there', 'your', "doesn't", 'of', 'isn', 'before', 'are', "didn't", "you're", "haven't", 'myself', 'for', "that'll", 'me', 'until', 'yourself', 'herself', 'those', 'because', 'about', 'shouldn', 'that', 'do', 'through', 'such', 'most', 'ourselves', 'above', 'it', 'hadn', 'couldn', 'theirs', 'this', 'them', 'a', 'haven', 'ours', 've', 'too', "shouldn't", 'her', 'own', "wouldn't", "mustn't", 'been', 'while', 'the', 'whom', 'where', 'between', 'into', 'with', 'be', 're', 'by', 'has', 'under', 'only', 'can', 'hers', 'which', "mightn't", 'aren', 'his', "won't", 'i', "don't", 'each', 'in', 'further', 'mustn', 'yourselves', 'what', 'itself', 'not', 'more', 'him', 'he', 'to', 'over', 'just', 'yours', 'at', 'being', 'both', 'wasn', 'why', 'as', 'who', 'does', 'hasn', "isn't", 'should', 'off', 'then', 'how', 'o', "hadn't", 's', 'll', 'she', "hasn't", 'our', 'wouldn', 'm', 'all', 'was', 'didn', 'mightn', 'my', 'than', 'same', 'ma', 'below', 'but', 'down', 'shan', 'had', 'these', 'no', 'any', "you've", "you'd", 'himself', 'you', 'doesn', 'from', 'its', 'nor', 'needn', "should've", 'have', 'on', 'when', "weren't", 'if', "couldn't", 'out', 'very', "it's", 'after',"co","de","I","n","","e","di","19","da","v","se","r","la","en","yg","1","2","k","n","k","a","l","3","ya","eu","em","f","4","0","g" ]

        conf = SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2")
        sc = SparkContext(conf=conf)
        spark = SparkSession.builder.appName("myApp") \
            .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/shop.corona16") \
            .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/shop.corona16") \
            .getOrCreate()
        self.df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()



    def t1(self,country):
        if(country=="all"):
            #val={}
            agr = [{"$group": {"_id": "$country", "count": {"$sum": 1}}},{"$sort": SON([("count", -1), ("_id", -1)])}]
            return self.coll.aggregate(agr)
            #return val
        else:
            country1=country.split(",")
            val1=[]
            for country2 in country1:
                agr=self.coll.find({"country":country2}).count()
                val1.append({"country":country2,"count":agr})
            return val1

    def t1_spark(self,country):
        if(country=="all"):
            q1_spark=self.df.groupBy("country").count()
            val1_spark=q1_spark.collect()
            return dict(val1_spark)
        else:
            country1=country.split(",")
            val1_spark=[]
            for country2 in country1:
                q1_spark=self.df.filter(col("country")==country2).count()
                val1_spark.append({"country":country2,"count":q1_spark})
            return val1_spark


    def t2(self,country):
        if(country == "all"):
            exp = [{"$group": {"_id": {"day": "$day", "month": "$month", "country": "$country"},"count": {"$sum": 1}}}]
            all_tasks2 = self.coll.aggregate(exp)
            return all_tasks2
        else:
            val2=[]
            country1=country.split(",")
            for country2 in country1:
                exp =[
                    {"$group": {"_id": {"day": "$day", "month": "$month","country":"$country"},"count": {"$sum": 1},"sourceA":{"$country":"$source"}}},
                    {"$project":{"source":{"$cond":[{"$eq":["$sourceA",country2]}]}}}

                ]
                all_tasks2=self.coll.aggregate(exp)
                #val2.append(jsonify(all_tasks2))
                val2=all_tasks2
            return val2





    def t2_spark(self,country):
        if(country=="all"):
            q2_spark=self.df.groupBy("country","day","month").count().sort(col("day"))
            return q2_spark.collect()
        else:
            country1=country.split(",")
            val2_spark=[]
            for country2 in country1:
                q2_spark=self.df.filter(col("country")==country2).groupBy("country","day","month").count().sort(col("day")).select("day","month","count")
                q2_spark_j=q2_spark.collect()
                for x in q2_spark_j:
                    #y="{day:"+x[0]+",month:"+x[1]+",count:"+str(x[2])+"}"
                    val2_spark.append({"country":country2,"data":x})
            return val2_spark




    def t3(self,limit):
        #en_stops = list(set(stopwords.words('english')))

        en_stops=["needn't", 'were', 'ain', 'don', 'did', 'or', 'they', 'will', "she's", "aren't", 'an', 'd', 'weren', 'won', "you'll", 'their', 'am', 'having', 'is', 'so', 'once', 'we', "shan't", 'and','Corona','Virus', 'again', 'during', 'other', 'doing', 'now', 'y', 'up', "wasn't", 'themselves', 't', 'few', 'some', 'against', 'here', 'there', 'your', "doesn't", 'of', 'isn', 'before', 'are', "didn't", "you're", "haven't", 'myself', 'for', "that'll", 'me', 'until', 'yourself', 'herself', 'those', 'because', 'about', 'shouldn', 'that', 'do', 'through', 'such', 'most', 'ourselves', 'above', 'it', 'hadn', 'couldn', 'theirs', 'this', 'them', 'a', 'haven', 'ours', 've', 'too', "shouldn't", 'her', 'own', "wouldn't", "mustn't", 'been', 'while', 'the', 'whom', 'where', 'between', 'into', 'with', 'be', 're', 'by', 'has', 'under', 'only', 'can', 'hers', 'which', "mightn't", 'aren', 'his', "won't", 'i', "don't", 'each', 'in', 'further', 'mustn', 'yourselves', 'what', 'itself', 'not', 'more', 'him', 'he', 'to', 'over', 'just', 'yours', 'at', 'being', 'both', 'wasn', 'why', 'as', 'who', 'does', 'hasn', "isn't", 'should', 'off', 'then', 'how', 'o', "hadn't", 's', 'll', 'she', "hasn't", 'our', 'wouldn', 'm', 'all', 'was', 'didn', 'mightn', 'my', 'than', 'same', 'ma', 'below', 'but', 'down', 'shan', 'had', 'these', 'no', 'any', "you've", "you'd", 'himself', 'you', 'doesn', 'from', 'its', 'nor', 'needn', "should've", 'have', 'on', 'when', "weren't", 'if', "couldn't", 'out', 'very', "it's", 'after',"co","de","I","n","","e","di","19","da","v","se","r","la","en","yg","1","2","k","n","k","a","l","3","ya","eu","em","f","4","0","g" ]

        other_words = ['de', 'la', 'que', 'en', 'el', '&amp;', '-', 'por',
                       'del', 'los', 'se', 'e', 'le', 'con', '|', 'es', 'da',
                       'una', 'les', 'al', 'em', 'un', 'para', 'las']
        exp = [{"$project": {"tweet": {"$split": ["$tweet", " "]}}},
               {"$unwind": "$tweet"},
               {"$group": {"_id": "$tweet", "total": {"$sum": 1}}},
               {"$sort": {"total": -1}},
               {"$match" :
                   {"$and": [
                       {'_id' : {'$ne' : ""}},
                       {'_id' : {"$nin" : en_stops}},
                       {'_id': {'$nin': other_words}},
                       # {'_id':{ "$gt": [ { "$strLenCP": "$_id" }, 2] }},


                   ]}},
               {"$redact":
                   {"$cond": [
                        { "$gt": [ { "$strLenCP": "$_id" }, 2] },
                           "$$KEEP",
                           "$$PRUNE"
                    ]}},
                {"$limit": limit}]
        val3 = self.coll.aggregate(exp, allowDiskUse=True)
        return  val3

    def t3_spark(self,limit):
        stopWords=["needn't", 'were', 'ain', 'don', 'did', 'or', 'they', 'will', "she's", "aren't", 'an', 'd', 'weren', 'won', "you'll", 'their', 'am', 'having', 'is', 'so','Corona','Virus', 'once', 'we', "shan't", 'and', 'again', 'during', 'other', 'doing', 'now', 'y', 'up', "wasn't", 'themselves', 't', 'few', 'some', 'against', 'here', 'there', 'your', "doesn't", 'of', 'isn', 'before', 'are', "didn't", "you're", "haven't", 'myself', 'for', "that'll", 'me', 'until', 'yourself', 'herself', 'those', 'because', 'about', 'shouldn', 'that', 'do', 'through', 'such', 'most', 'ourselves', 'above', 'it', 'hadn', 'couldn', 'theirs', 'this', 'them', 'a', 'haven', 'ours', 've', 'too', "shouldn't", 'her', 'own', "wouldn't", "mustn't", 'been', 'while', 'the', 'whom', 'where', 'between', 'into', 'with', 'be', 're', 'by', 'has', 'under', 'only', 'can', 'hers', 'which', "mightn't", 'aren', 'his', "won't", 'i', "don't", 'each', 'in', 'further', 'mustn', 'yourselves', 'what', 'itself', 'not', 'more', 'him', 'he', 'to', 'over', 'just', 'yours', 'at', 'being', 'both', 'wasn', 'why', 'as', 'who', 'does', 'hasn', "isn't", 'should', 'off', 'then', 'how', 'o', "hadn't", 's', 'll', 'she', "hasn't", 'our', 'wouldn', 'm', 'all', 'was', 'didn', 'mightn', 'my', 'than', 'same', 'ma', 'below', 'but', 'down', 'shan', 'had', 'these', 'no', 'any', "you've", "you'd", 'himself', 'you', 'doesn', 'from', 'its', 'nor', 'needn', "should've", 'have', 'on', 'when', "weren't", 'if', "couldn't", 'out', 'very', "it's", 'after',"co","de","I","n","","e","di","19","da","v","se","r","la","en","yg","1","2","k","n","k","a","l","3","ya","eu","em","f","4","0","g" ]
        q3_spark = self.df.withColumn("word", explode ( split("tweet", " "))).groupBy("word").count().filter((~col('word').isin(stopWords))).sort(col("count").desc())
        #q3.show(limit)
        #val3_spark = json.loads(q3.toJSON())
        #val3_spark = q3.head(limit).toJSON()


        val3_spark=q3_spark.collect()[:limit]
        return {"words":dict(val3_spark)}
        #return Response(val3_spark.to_json(orient="columns"), mimetype='application/json')








    def t4(self,limit,country):
        #en_stops = list(set(stopwords.words('english')))
        en_stops=["needn't", 'were', 'ain', 'don', 'did', 'or', 'they', 'will', "she's", "aren't", 'an', 'd', 'weren', 'won', "you'll", 'their', 'am', 'having', 'is', 'so', 'once', 'we', "shan't", 'and','Corona','Virus', 'again', 'during', 'other', 'doing', 'now', 'y', 'up', "wasn't", 'themselves', 't', 'few', 'some', 'against', 'here', 'there', 'your', "doesn't", 'of', 'isn', 'before', 'are', "didn't", "you're", "haven't", 'myself', 'for', "that'll", 'me', 'until', 'yourself', 'herself', 'those', 'because', 'about', 'shouldn', 'that', 'do', 'through', 'such', 'most', 'ourselves', 'above', 'it', 'hadn', 'couldn', 'theirs', 'this', 'them', 'a', 'haven', 'ours', 've', 'too', "shouldn't", 'her', 'own', "wouldn't", "mustn't", 'been', 'while', 'the', 'whom', 'where', 'between', 'into', 'with', 'be', 're', 'by', 'has', 'under', 'only', 'can', 'hers', 'which', "mightn't", 'aren', 'his', "won't", 'i', "don't", 'each', 'in', 'further', 'mustn', 'yourselves', 'what', 'itself', 'not', 'more', 'him', 'he', 'to', 'over', 'just', 'yours', 'at', 'being', 'both', 'wasn', 'why', 'as', 'who', 'does', 'hasn', "isn't", 'should', 'off', 'then', 'how', 'o', "hadn't", 's', 'll', 'she', "hasn't", 'our', 'wouldn', 'm', 'all', 'was', 'didn', 'mightn', 'my', 'than', 'same', 'ma', 'below', 'but', 'down', 'shan', 'had', 'these', 'no', 'any', "you've", "you'd", 'himself', 'you', 'doesn', 'from', 'its', 'nor', 'needn', "should've", 'have', 'on', 'when', "weren't", 'if', "couldn't", 'out', 'very', "it's", 'after',"co","de","I","n","","e","di","19","da","v","se","r","la","en","yg","1","2","k","n","k","a","l","3","ya","eu","em","f","4","0","g" ]

        country1=country
        other_words = ['de', 'la', 'que', 'en', 'el', '&amp;', '-', 'por',
                       'del', 'los', 'se', 'e', 'le', 'con', '|', 'es', 'da',
                       'una', 'les', 'al', 'em', 'un', 'para', 'las']
        val4 = self.coll.aggregate([
            {"$project": {"country": "$country", "word": {"$split": ["$tweet", " "]}}},
            {"$unwind": "$word"},
            {'$project': {'word': { '$toLower': "$word"}, 'country': 1}},
            {'$match': {
                '$and': [
                    {'word' : {"$toLower":"word"}},
                    {'word': {'$ne': ""}},
                    {'word': {'$nin': en_stops}},
                    {'word': {'$nin': other_words}}
                ]}},
            {"$group": {"_id": {"country": "$country", "word": "$word"}, "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
            {"$group": {"_id": "$_id.country", "Top_Words": {"$push": {"word": "$_id.word", "total": "$total"}}}},
            {"$project": {"country": 1, "top100Words": {"$slice": ["$Top_Words", limit]}}}
        ], allowDiskUse=True)
        #val4 = (exp)
        return val4

    def t4_spark(self,limit,country):
        stopWords=["needn't", 'were', 'ain', 'don', 'did', 'or', 'they', 'will', "she's", "aren't", 'an', 'd', 'weren', 'won', "you'll", 'their', 'am', 'having', 'is', 'so', 'once', 'we', "shan't", 'and','Corona','Virus', 'again', 'during', 'other', 'doing', 'now', 'y', 'up', "wasn't", 'themselves', 't', 'few', 'some', 'against', 'here', 'there', 'your', "doesn't", 'of', 'isn', 'before', 'are', "didn't", "you're", "haven't", 'myself', 'for', "that'll", 'me', 'until', 'yourself', 'herself', 'those', 'because', 'about', 'shouldn', 'that', 'do', 'through', 'such', 'most', 'ourselves', 'above', 'it', 'hadn', 'couldn', 'theirs', 'this', 'them', 'a', 'haven', 'ours', 've', 'too', "shouldn't", 'her', 'own', "wouldn't", "mustn't", 'been', 'while', 'the', 'whom', 'where', 'between', 'into', 'with', 'be', 're', 'by', 'has', 'under', 'only', 'can', 'hers', 'which', "mightn't", 'aren', 'his', "won't", 'i', "don't", 'each', 'in', 'further', 'mustn', 'yourselves', 'what', 'itself', 'not', 'more', 'him', 'he', 'to', 'over', 'just', 'yours', 'at', 'being', 'both', 'wasn', 'why', 'as', 'who', 'does', 'hasn', "isn't", 'should', 'off', 'then', 'how', 'o', "hadn't", 's', 'll', 'she', "hasn't", 'our', 'wouldn', 'm', 'all', 'was', 'didn', 'mightn', 'my', 'than', 'same', 'ma', 'below', 'but', 'down', 'shan', 'had', 'these', 'no', 'any', "you've", "you'd", 'himself', 'you', 'doesn', 'from', 'its', 'nor', 'needn', "should've", 'have', 'on', 'when', "weren't", 'if', "couldn't", 'out', 'very', "it's", 'after',"co","de","I","n","","e","di","19","da","v","se","r","la","en","yg","1","2","k","n","k","a","l","3","ya","eu","em","f","4","0","g" ]
        country1=country.split(",")
        val4_sp=[]
        for country2 in country1:
            q4_spark=self.df.filter(col('country')==country2).withColumn("word", explode ( split("tweet", " "))).groupBy("word").count().filter((~col('word').isin(stopWords))).sort(col("count").desc())
            val4_spark=q4_spark.collect()[:limit]
            #for x in val4_spark:
            val4_sp.append({"country":country2,"words":dict(val4_spark)})

        return val4_sp
