from pyspark.sql import SparkSession, functions as F
import requests
url = 'https://api.plos.org/search?q=title:DNA'
r = requests.get(url, verify=False)
jsonData = r.json()
print(jsonData)
jsonData = jsonData['response']['docs']
print('jsonData:',jsonData['response']['docs'])
spark = SparkSession.builder.getOrCreate()
rdd = spark.sparkContext.parallelize([jsonData])
df = spark.read.json(rdd)
print(df.head())
