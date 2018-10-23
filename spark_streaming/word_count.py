from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(master='local[4]')
ssc = StreamingContext(sc, batchDuration=10)

dstream = ssc.textFileStream('file:///home/artvybor/test_words')

words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
result = pairs.reduceByKey(lambda x, y: x + y)



result.pprint()

ssc.start()
ssc.awaitTermination()
