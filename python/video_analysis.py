from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkConf,SparkContext
import json



if __name__ == "__main__":
    spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("YoutubeAnalytics").getOrCreate()
    sqlContext = SQLContext(spark)
    
    
        
    #the data in 2017 has the tag statistics. 
    #lines = sqlContext.read.json("D:/Developer/Midgame/datasets/youtube-scraper-1-2016-08-31-18-12-10-d8585151-f7d3-45e0-bcc9-afc8c840c880")   
    #the preview line does not has the tag statistics 
    #lines = sqlContext.read.json("D:/Developer/Midgame/datasets/youtube-scraper-3-2017-01-06-17-28-52-3e9bfa2b-d8a2-45b4-8a30-3a30a5160aad") 
    lines = sqlContext.read.json("D:/Developer/Midgame/datasets/youtube-scraper-3-2017-01-06-23-43-08-f3ee4357-bbc5-4a88-8bab-39433791168a")      
    print(lines.printSchema())
    data = lines.select("items.id", "items.statistics.commentCount", "items.statistics.dislikeCount", "items.statistics.hiddenSubscriberCount", "items.statistics.likeCount", \
    "items.statistics.subscriberCount", "items.statistics.videoCount", "items.statistics.viewCount")
    #print(data)    
    data = data.withColumn("id", data["id"].getItem(0).cast("string"))  
    data = data.withColumn("commentCount", data["commentCount"].getItem(0).cast("string"))
    data = data.withColumn("dislikeCount", data["dislikeCount"].getItem(0).cast("string"))
    data = data.withColumn("hiddenSubscriberCount", data["hiddenSubscriberCount"].getItem(0).cast("string"))
    data = data.withColumn("likeCount", data["likeCount"].getItem(0).cast("string"))
    data = data.withColumn("subscriberCount", data["subscriberCount"].getItem(0).cast("string"))
    data = data.withColumn("videoCount", data["videoCount"].getItem(0).cast("string"))
    data = data.withColumn("viewCount", data["viewCount"].getItem(0).cast("string"))
    print(data.first())    
    
    #data.write.csv('mycsv-1-2016-08-31-18-12-10-.csv')
    #the data in 2016 has another schema different to 2017 (it has information about ghe author)
    