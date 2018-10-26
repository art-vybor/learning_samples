from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(master='local[4]')
ssc = StreamingContext(sc, batchDuration=10)

dstream = ssc.socketTextStream(hostname='localhost', port=9999)

result = dstream.filter(bool).count()

result.pprint()

ssc.start()
ssc.awaitTermination()