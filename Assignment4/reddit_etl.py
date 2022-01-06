from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def add_pairs(a, b):
    return a[0] + b[0], a[1] + b[1]


def get_avg(subreddit):
    subreddit_name, values = subreddit
    return json.dumps((subreddit_name, (values[1]/values[0])))


def convert_to_json(comment):
    comment_json = json.loads(comment)
    return comment_json['subreddit'], comment_json['score'], comment_json['author']


def main(inputs, output):
    comments = sc.textFile(inputs)
    json_comments = comments.map(convert_to_json)
    json_comments_with_e = json_comments.filter(lambda comment: 'e' in comment[0]).cache()
    positive_score_comments = json_comments_with_e.filter(lambda comment: comment[1] > 0)
    negative_score_comments = json_comments_with_e.filter(lambda comment: comment[1] < 0)
    positive_score_comments.map(json.dumps).saveAsTextFile(output + '/positive')
    negative_score_comments.map(json.dumps).saveAsTextFile(output + '/negative')


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)