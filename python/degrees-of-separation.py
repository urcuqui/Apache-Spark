#Breadth First Search algorithm in Spark 

#import libraries
from pyspark import SparkContext, SparkConf
 
 #configuration
conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf =  conf)

#the first and last node are sknown, so, the next line defines what is 
#the start point and the target in order to calculate the distance between them
startCharacterID = 5306 #SpiderMan
targetCharacterID = 14 #ADAM 

#the accumulator is used be a signal of target character
#throught the BFS is going to process 
hitCounter = sc.accumulator(0)

# the next method is dedicated to convert our data in a network integrated by nodes, connections and their colors
# this graph will be a tuple of connections, distances and colors; they will have a default values 
 
def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    if (heroID == startCharacterID):
        color = 'GRAY'
        distance = 0

    return (heroID, (connections, distance, color))

# the next method gets the text file and it will call the method to convert it in a BFS
def createStartingRdd():
    inputFile = sc.textFile("file:///D:/Developer/Apache-Spark/datasets/marvel-graph.txt")
    return inputFile.map(convertToBFS)

#
def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    #If this node needs to be expanded...
    if (color == 'GRAY'):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if (targetCharacterID == connection):
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        #We've processed this node, so color it black
        color = 'BLACK'

    #Emit the input node so we don't lose it.
    results.append( (characterID, (connections, distance, color)) )
    return results

def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Preserve minimum distance
    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (edges, distance, color)


#Main program here:
if __name__ == "__main__":
    iterationRdd = createStartingRdd()
    
    # it has the hypothesis that with 
    for iteration in range(0, 10):
        print("Running BFS iteration# " + str(iteration+1))
    
        # Create new vertices as needed to darken or reduce distances in the
        # reduce stage. If we encounter the node we're looking for as a GRAY
        # node, increment our accumulator to signal that we're done.
        mapped = iterationRdd.flatMap(bfsMap)
    
        # Note that mapped.count() action here forces the RDD to be evaluated, and
        # that's the only reason our accumulator is actually updated.
        print("Processing " + str(mapped.count()) + " values.")
    
        if (hitCounter.value > 0):
            print("Hit the target character! From " + str(hitCounter.value) \
                + " different direction(s).")
            break
    
        # Reducer combines data for each character ID, preserving the darkest
        # color and shortest path.
        iterationRdd = mapped.reduceByKey(bfsReduce)

    
        
             
             