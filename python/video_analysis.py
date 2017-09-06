from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark import SparkConf,SparkContext
import json

def loadYoutubeInformation(f):
    youtubeInfo = {}
    #with open("", encoding='utf-8', errors='ignore') as f:
    for line in f:
        print (line)
        data =  json.loads(str(line))
        try:
            idVideo = data[0]["items"][0]["id"].encode("utf-8")
        except:
            idVideo = ''
        try:
            viewsCount = (data[0]["items"][0]["statistics"]["viewCount"].encode("utf-8"))
        except :
            viewsCount = 0
        try:
            commentsCount = (data[0]["items"][0]["statistics"]["commentsCount"].encode("utf-8"))
        except :   
            commentsCount = 0
        try:
            suscriberCount = (data[0]["items"][0]["statistics"]["suscriberCount"].encode("utf-8"))
        except :   
            suscriberCount = 0
        try:
            videoCount = (data[0]["items"][0]["statistics"]["videoCount"].encode("utf-8"))
        except :   
            videoCount = 0            
        return Row(idVideo=str(idVideo),viewsCount=int(viewsCount),commentsCount=int(commentsCount),suscriberCount=int(suscriberCount),  \
        videoCount=int(videoCount) ) 
        
            
    
    
    #sqlContext = SQLContext(spark)
    #df = sqlContext.read.json('D:/Developer/Midgame/datasets/youtube-scraper-3-2017-01-06-17-28-52-3e9bfa2b-d8a2-45b4-8a30-3a30a5160aad')
    
#    print(df.first())
            

#if __name__ == "__main__":
#spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("YoutubeAnalytics").getOrCreate()
conf = SparkConf().setMaster("local").setAppName("YoutubeAnalytics")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
df = sqlContext.read.json("G:/Research/Markwatch/Datasets/youtube-scraper-2-2016-09-23-16-55-40-a049f820-7b94-4682-97cc-6dbd280e7e1a")
items = (df.select("items"))
sqlContext.registerDataFrameAsTable(items, "itemsyoutube")
print(items.printSchema())
statistics = sqlContext.sql("SELECT element FROM itemsyoutube")
#a = other.select("element")
#df.registerTempTable("youtube")
#statistics = sqlContext.sql("SELECT items FROM youtube")
#print(statistics.printSchema())
#statistics = sqlContext.sql("SELECT element FROM youtube")

#data = lines.map(lines)
#lines.registerTempTable("youtube")
#statistics = sqlContext.sql("SELECT items FROM youtube").collect()
#print (statistics)
#print(statistics.first().encode("utf-8"))
#results = data.collect()




#parts = lines.map(lambda p: Row(
#print(lines.columns)
#print(lines.printSchema())

# people = lines.map(loadYoutubeInformation)
# schemaPeople = spark.createDataFrame(people).cache()
# schemaPeople.createOrReplaceTempView("youtube")
# teenagers = spark.sql("SELECT idVideo FROM youtube")
# for teen in teenagers.collect():
#     print(teen)
#     
#spark.stop()
