from pyspark import SparkConf,SparkContext
import re
conf =  SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf =conf)
#this is an example that uses Bag Words representation for text analytics
#Bag words has three steps in order to process tokens in the documents 
#1)stop words, this activity is dedicated to remove words that are no important
#for the analysis, their meaning do not represent a relevance
#2)normalization, it is dedicated to change tokens in their lowercase form 
#3)steamming, the last step is dedicated to change tokens to their base form
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())    

input =sc.textFile("file:///G:/Research/Apache-Spark/datasets/book.txt")
#flatMap has the ability to blow out and RDD into multiple elements  
words = input.flatMap(normalizeWords)

wordCounts =  words.map(lambda x:(x,1)).reduceByKey(lambda x, y: x+y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

results =  wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print ((word.decode() + ":\t\t") + str(count))


#wordCounts = words.countByValue()


# for word, count in wordCounts.items():
#     cleanWord = word.encode('ascii','ignore')
#     if(cleanWord):
#         print (cleanWord,count)