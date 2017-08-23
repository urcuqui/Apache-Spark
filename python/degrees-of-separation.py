#Breadth First Search algorithm in Spark 

#import libraries
from pyspark import SparkContext, SparkConf
 
 #configuration
conf = SparkConf().setMaster("local").setAppName("RelationHeroe")
sc = SparkContext(conf =  conf)

#the first and last node are sknown, so, the next line defines what is 
#the start point and the target in order to calculate the distance between them
startCharacterID = 5360 #SpiderMan
targetCharacterID = 14 #ADAM 

#the accumulator is used be a signal of target character
#throught the BFS is going to process 
hitCounter = sc.accumulator(0)

#this is the method to convert the line in a graph of BFS
def convertToBPS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections =  []
    for connection in fields[1:]:
        connections.append(int(connection))
    #color white is for the nodes that have not been processed 
    color ='WHITE'
    distance =  9999
    if (heroID == startCharacterID):
        #Gray is the color that the target node will use 
        color =  'GRAY'
        distance = 0
    
    return (heroID,(connections, distance, color))
    
def createStartingRdd():
    inputFile =sc.textFile("file:///D:/Developer/Apache-Spark/datasets/Marvel-Graph.txt")
    return inputFile.map(convertToBPS)

def bfsMap(node):
    characterID = node[0]
    data =  node[1]
    connections =  data[0]
    distance = data[1]
    color = data[2]
    
    results = []
    
    #If this node needs to be expanded...
    if (color == 'GRAY'):
        for connection in connections:
            newCharacterID =  connection 
            newDistance = distance +1
            newColor =  'GRAY'
            if (targetCharacterID == connection):
                hitCounter.add(1)
                
             newEntry = (newCharacterID, ([], newDistance, newColor))
             results.append(newEntry)
             
            #When the node has processed, so color for it is black 
            color = 'BLACK'
            
        #Emit the input node so we do not lose it.
        results.append((character,(connections,distance,color))
        return (results)
    
def bfsReduce(data1, data2):
    edges1 =  data1[0]
    edges2 =  data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]
    
    distance = 9999
    color = 'WHITE'
    edges = []
    
    # See if one is the original node with its connections 
    # If so preserve them
    if (len[edges1[ >0):
    edges = edges1
    elif (len(edges2) >0):
        edges =  edges2
    
    # Preserve minimum distance
    if (distance1 < distance):
        distance = distance1
        
    if (distance2 < distance):
        distance = distance2
    
    # Preserve darkest color
    if (color == 'WHITE' and (color2 == 'GRAY' or color2 == 'GRAY')):
        color =  color2

    if(color2 == 'GRAY' and color2 == 'GRAY'):
        color = color2
    
    return (edges, distance, color)
    
#Main program here:
iterationRdd = createStartingRdd()

for iteration in range(0,10):
    print ("Running BFS iteration# " +  str(iteration +1))

        
             
             