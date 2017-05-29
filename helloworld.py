from pyspark import SparkConf, SparkContext

# Launch standalone project
conf = SparkConf().setAppName("HelloWorld")
sc = SparkContext(conf = conf)
lines = sc.textFile("/opt/spark/README.md")
pythonLines = lines.filter(lambda line: "Python" in line)
result = pythonLines.collect()
print("================")
for line in result:
    print(line)
print("================")
