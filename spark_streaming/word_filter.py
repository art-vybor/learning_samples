from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(master='local[4]')
ssc = StreamingContext(sc, batchDuration=10)

dstream = ssc.textFileStream('file:///home/artvybor/test_words')

result = dstream.filter(bool).count()

result.pprint()

ssc.start()
ssc.awaitTermination()
