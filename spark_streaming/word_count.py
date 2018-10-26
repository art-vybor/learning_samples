from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(master='local[4]')
ssc = StreamingContext(sc, batchDuration=10)

def ls(directory):
    hadoop = sc._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    path = hadoop.fs.Path(directory)
    return [f.getPath() for f in fs.get(conf).listStatus(path)]

ls_result = ls('hdfs:///data/griboedov/')

batches = [sc.textFile(f) for f in map(str, ls_result)]
dstream = ssc.queueStream(rdds=batches)

def print_rdd(rdd):
	for row in rdd.take(10):
		print(row)
	print('=============================')

result = dstream \
	.flatMap(lambda line: line.split()) \
	.map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .foreachRDD(lambda rdd: print_rdd(rdd.sortBy(lambda x: -x[1])))

ssc.start()
ssc.awaitTermination()

# Preparing SparkContext
sc = SparkContext(master='local[4]')

# Preparing base RDD with the input data

