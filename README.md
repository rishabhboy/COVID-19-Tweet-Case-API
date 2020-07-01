# API for analysis of Covid'19 through Twitter Streaming Data

### Twitter Producer:
The Twitter Producer gets data from Twitter, based on some keywords and put them in a Kafka topic.

### Mongodb:
Your Consumer component should get data from your twitter Kafka topic and insert it into Mongodb after doing 
some basic transformations .

### Functionalities:
Need to find overall number of tweets on coronavirus (keywords can be virus/covid-19/corona etc. ) per country in the last 3 months .

Next, need to find overall number of tweets per country on a daily basis.

Also need toduo find the top 100 words occurring on tweets involving coronavirus. (words should be nouns/verbs and not involving common ones like the, is, are, etc etc).

Find the top 100 words occurring on tweets involving coronavirus on a per country basis.




### Requirement Analysis
* We have used Java to write twitter producer code that fetches tweets from twitter api and stores in kafka topic.

* Similarly, Java is used to write twitter consumer code through which tweets are stored in Mongodb from kafka topic through HTTP Request.

* We preferred mongodb over sql database because of effective scalability, elasticity and faster storage(latency) of the former.

* In order to solve the first two functionalities, we created api using python’s Flask framework  as flask maps HTTP requests to Python function. Furthermore, we have used pyspyspark for 3rd and 4th use cases . Using pyspark enabled  us to solve those use cases in even lesser time.

* We used postman to run the queries written in api.


### Architecture depicting the flow of Twitter data
Twitter data is stored in Mongodb and then further queried .
 In our database, we have stored data in form of 4  fields 
 namely {“country”, “tweet”, “day”, “month”}. Then the 
 functionalities are performed on this json data through 
 api in order to solve these use cases .Once queried, output 
 of these use cases can be seen through postman tool by entering 
 the correct path for api.
