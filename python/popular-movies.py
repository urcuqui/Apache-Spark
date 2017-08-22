from pyspark import SparkContext, SparkConf

#the next method creates a dictionary about the movie names registered in another file
def loadMovieNames():
    movieNames = {}
    with open("G:/Research/Apache-Spark/datasets/ml-100k/u.ITEM") as f:
        for line in f:
            fields =  line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

#configuration
conf = SparkConf().setMaster("local").setAppName("PopulaMovies")
sc = SparkContext(conf =  conf)

#broadcast with movie names, the idea is to share the dictionary with the cluster
nameDict = sc.broadcast(loadMovieNames())

lines =  sc.textFile("file:///G:/Research/Apache-Spark/datasets/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]),1))
movieCounts = movies.reduceByKey(lambda x,y : x+y)

sortedMovies = movieCounts.map(lambda x: (x[1], x[0])).sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print(result)
    