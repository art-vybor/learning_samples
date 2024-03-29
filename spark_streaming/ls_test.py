from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(master='local[4]')

def ls(directory):
    hadoop = sc._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    path = hadoop.fs.Path(directory)
    return [f.getPath() for f in fs.get(conf).listStatus(path)]

ls_result = ls('hdfs:///data/course4//uid_ua_100k_splitted_by_5k/')
files = list(map(str, ls_result))

print('\n'.join(files))
print('============================================')

print('\n'.join(sc.textFile(files[0]).take(10)))

