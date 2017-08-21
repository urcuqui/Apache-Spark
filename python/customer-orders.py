from pyspark import SparkConf,SparkContext
import collections

conf =  SparkConf().setMaster("local").setAppName("customerOrders")
sc = SparkContext(conf =conf)#

input =sc.textFile("file:///G:/Research/Apache-Spark/datasets/customer-orders.csv")

# the next method parses the lines by the commas
def parseLine(line):
    fields= line.split(',')
    customerID=fields[0]
    spent=fields[2]    
    return (int(customerID), float(spent))

parsedLines =input.map(parseLine)
#collect the values by each client in the list
spents = parsedLines.reduceByKey(lambda x, y: x+y)
#the next line sorts the clients by each spent
clientsSortedBySpent = spents.map(lambda x: (x[1], x[0])).sortByKey()
results = clientsSortedBySpent.collect()

for result in results:
    customer = result[0]
    spent = result[1]
    print("spent:"+str(customer) + "; CustomerID:"+str(spent))