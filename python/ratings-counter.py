#the next two python libraries are dedicated to implement RDD's (Resilent, Distributed, Datasets), whose have RDD actions, cluster settings and others characteristics 
from pyspark import SparkConf, SparkContext
import collections

#the next lines set up the context and the local distribution
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

#load a textfile with a lot of rows and with four characteristics  
lines = sc.textFile("file:///G:/Research/Apache-Spark/datasets/ml-100k/u.data")
#it takes the ratings of each movie and it will count all of them respectively,
#in order to calculate the total rating by movie. On the other hand, the 
#array indexes start at 0 
ratings = lines.map(lambda x: x.split()[2])
#countByValue counts the number of times that a value appears more than once 
result = ratings.countByValue()
#the next lines will sort the array by movie and they will show the results
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
