from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def split_stats(line):
    timestamp, lang, title, view_count, bytes_returned = line.split()
    view_count = int(view_count)
    # print(timestamp, lang, title, view_count, bytes_returned)
    return (timestamp, lang, title, view_count, bytes_returned)

def map_stats(stats):
    return (stats[0], (stats[3], stats[2]))

def get_key(kv):
    return kv[0]

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

text = sc.textFile(inputs)
splitted_stats = text.map(split_stats)
# print(splitted_stats.take(10))
en_filtered_stats = splitted_stats.filter(lambda stat: stat[1] == "en")
title_filtered_stats_1 = en_filtered_stats.filter(lambda stat: stat[2] != "Main_Page")
title_filtered_stats_2 = title_filtered_stats_1.filter(lambda stat: not stat[2].startswith("Special:"))
mapped_stats = title_filtered_stats_2.map(map_stats)
# print(mapped_stats.take(10))
reduced_stats = mapped_stats.reduceByKey(max)
# print(reduced_stats.take(10))

outdata = reduced_stats.sortBy(get_key).map(tab_separated)
outdata.saveAsTextFile(output)