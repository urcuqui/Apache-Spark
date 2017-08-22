#import libraries
from pyspark import SparkContext, SparkConf
 
#configuration
conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf =  conf)

def countCoOccurrences(line):
    elements =  line.split()
    return (int(elements[0]), len(elements) -1)
    
def parseNames(lines):
    fields = lines.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

names = sc.textFile("file:///D:/Developer/Apache-Spark/datasets/Marvel-Names.txt")
namesRdd = names.map(parseNames)

lines =  sc.textFile("file:///D:/Developer/Apache-Spark/datasets/Marvel-Graph.txt")

pairings = lines.map(countCoOccurrences)

totalFriendsByCharacter = pairings.reduceByKey(lambda x,y : x+y)
flipped = totalFriendsByCharacter.map(lambda x: (x[1], x[0]))

mostPopular =  flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print (mostPopularName.decode() + " is the most popular superhero, with " + str(mostPopular[0]) + " co-appearances.")
