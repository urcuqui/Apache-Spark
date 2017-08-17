from pyspark import SparkConf, SparkContext
import collections

#app name
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

#load a textfile with a lot of rows and with four characteristics  
lines = sc.textFile("file:///G:/Research/Apache-Spark/ml-100k/u.data")
#it takes the ratings of each movie and it will count all of them respectively,
#in order to calculate the total rating by movie 
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()
#the next lines will sort the array by movie and they will show the results
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
