import sys
from pyspark import SparkContext, SparkConf
if __name__ == "__main__":

	# create Spark context with necessary configuration
	sc = SparkContext(appName='wordCount')
	# read data from text file and split each line into words
	words = sc.textFile("s3://aws-logs-710114258989-us-west-2/elasticmapreduce/food_items.txt").flatMap(lambda line: line.split(","))
	# count the occurrence of each word
	wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)

for x in wordCounts.collect():
	print(x)