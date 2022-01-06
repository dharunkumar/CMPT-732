from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def add_pairs(a, b):
    return (a[0] + b[0], a[1] + b[1])


def get_avg(subreddit):
    subreddit_name, values = subreddit
    return json.dumps((subreddit_name, (values[1]/values[0])))


def convert_to_json(sub_reddit):
    sub_reddit_json = json.loads(sub_reddit)
    return (sub_reddit_json['subreddit'], (1, sub_reddit_json['score']))


def main(inputs, output):
    # input = json.loads(inputs)
    sub_reddits = sc.textFile(inputs)
    json_sub_reddits = sub_reddits.map(convert_to_json)
    # print(json_sub_reddits.take(10))
    reduced_values = json_sub_reddits.reduceByKey(add_pairs)
    # print(reduced_values.take(10))
    avg_values = reduced_values.map(get_avg)
    # print(avg_values)
    avg_values.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)