#import the libraries
from pyspark import SparkConf, SparkContext
import collections

conf =  SparkConf().setMaster("local").setAppName("Friends")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields =  line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age,numFriends)

lines= sc.textFile("file:///D:/Developer/Apache-Spark/datasets/fakefriends.csv")
rdd =  lines.map (parseLine)

#Count up sum of friends and number of entries per age
totalsByAge = rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
#the avarage will be calculated by values assigned for each key 
averagesByAge =  totalsByAge.mapValues(lambda x:x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print (result)

