from pyspark import SparkConf,SparkContext


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

results = spents.collect()
for result in results:
    customer = result[0]
    spent = result[1]
    print("id:"+str(customer) + " spent:"+str(spent))