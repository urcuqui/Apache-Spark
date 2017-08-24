#Filtering and recommendations models based on collaborative approach 

from pyspark import SparkContext, SparkConf
import sys
from math import sqrt

#the next method creates a dictionary about the movie names registered in another file
def loadMovieNames():
    movieNames = {}
    with open("D:/Developer/Apache-Spark/datasets/ml-100k/u.ITEM") as f:
        for line in f:
            fields =  line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')            
    return movieNames
    
# It makes the pairs of user and the ratings of each movie
def makePairs(user,ratings):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

def filterDuplicates(userID, ratings):
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2
    
