from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(master='local[4]')
ssc = StreamingContext(sc, batchDuration=1)

def ls(directory):
    hadoop = sc._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    path = hadoop.fs.Path(directory)
    return [f.getPath() for f in fs.get(conf).listStatus(path)]

ls_result = ls('hdfs:///data/griboedov/')

batches = [sc.textFile(f) for f in map(str, ls_result)]
dstream = ssc.queueStream(rdds=batches)

result = dstream.filter(bool).count()

result.pprint()

ssc.start()
ssc.awaitTermination()
