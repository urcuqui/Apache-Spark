from pyspark import SparkContext, SparkConf
 
#configuration
conf = SparkConf().setMaster("local").setAppName("maxTemperature")
sc = SparkContext(conf =  conf)

lines =  sc.textFile("file:///G:/Research/Apache-Spark/datasets/1800.csv")


def parseLine(line):
    fields= line.split(',')
    stationID=fields[0]
    entryType=fields[2]
    #the temperature will be on Fahrenheit
    temperature =  float(fields[3])*0.1*(9.0/5.0)+32.0
    return (stationID, entryType, temperature)
    
parsedLines = lines.map(parseLine)
#filter the rows that only have TMAX
minTemps =  parsedLines.filter(lambda x: "TMAX" in x[1])
#eliminate the column entryType
stationTemps =  minTemps.map(lambda x: (x[0], x[2]))

#it finds the minimum temperature by stationID
minTemps = stationTemps.reduceByKey(lambda x,y: max(x,y))

results = minTemps.collect()
for result in results:
    print (result[0]+ "\t{:.2f}F".format(result[1]))

    