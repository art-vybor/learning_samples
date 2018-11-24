# coding: utf-8
from time import sleep
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from ua_parser import user_agent_parser
from hyperloglog import HyperLogLog


BATCH_TIMEOUT = 1 # Timeout between batch generation in seconds
DATA_PATH = "hdfs:///data/course4/uid_ua_100k_splitted_by_5k"


sc = SparkContext(master='local[4]')
ssc = StreamingContext(sc, BATCH_TIMEOUT)

def ls(directory):
    hadoop = sc._jvm.org.apache.hadoop

    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration() 
    path = hadoop.fs.Path(directory)

    return [f.getPath() for f in fs.get(conf).listStatus(path)]

batches = [sc.textFile(f) for f in map(str, ls(DATA_PATH))]
dstream = ssc.queueStream(rdds=batches)


finished = False
printed = False

def set_ending_flag(rdd):
    global finished
    if rdd.isEmpty():
        finished = True

def print_only_at_the_end(rdd):
    global printed
    rdd.count()  # dirty_hack; TODO: remove

    if finished and not printed:
        for word, count in rdd.takeOrdered(10, key=lambda x: -x[1]):
            print("{}\t{}".format(word, count))
        printed = True

dstream.foreachRDD(set_ending_flag)


def get_segments(uid, ua):
    parsed_ua = user_agent_parser.Parse(ua)
    result = []

    if parsed_ua['device']['family'] == 'iPhone':
        result.append(('seg_iphone', uid))
    if parsed_ua['user_agent']['family'] == 'Firefox':
        result.append(('seg_firefox', uid))
    if parsed_ua['os']['family'] == 'Windows':
        result.append(('seg_windows', uid))

    return result

def update_uids(new_uids, hll):
    hll = hll or HyperLogLog(0.01)
    
    for uid in new_uids:
        hll.add(uid)

    return hll

dstream.flatMap(lambda lines: lines.split('\n')) \
       .map(lambda line: line.split('\t')[:2]) \
       .flatMap(lambda uid_ua: get_segments(uid_ua[0], uid_ua[1])) \
       .updateStateByKey(update_uids) \
       .map(lambda segment_hll: (segment_hll[0], len(segment_hll[1]))) \
       .foreachRDD(print_only_at_the_end)

ssc.checkpoint('./checkpoint')
ssc.start()
while not printed:
    sleep(0.1)
ssc.stop()
sc.stop()
