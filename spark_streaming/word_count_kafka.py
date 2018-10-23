from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(master='local[4]')
ssc = StreamingContext(sc, batchDuration=10)

dstream = KafkaUtils.createDirectStream(
    ssc, topics=['test'],
    kafkaParams = {'metadata.broker.list': 'mipt-node06.atp-fivt.org:9092'}
)

# dstream = ssc.textFileStream('file:///home/artvybor/test_words')

result = dstream \
        .flatMap(lambda (_, line): line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda x, y: x + y)



result.pprint()

ssc.start()
ssc.awaitTermination()
