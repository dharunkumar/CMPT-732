from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def add_pairs(a, b):
    return a[0] + b[0], a[1] + b[1]


def get_avg(subreddit):
    subreddit_name, values = subreddit
    # return json.dumps((subreddit_name, (values[1]/values[0])))
    return subreddit_name, (values[1]/values[0])


def calculate_comment_values(comment):
    subreddit, comment_with_avg = comment
    comment_data, average = comment_with_avg
    return comment_data['score']/average, comment_data['author']


def map_reddit_score_data(sub_reddit):
    return sub_reddit['subreddit'], (1, sub_reddit['score'])


def map_comments(comment):
    return comment['subreddit'], comment


def main(inputs, output):
    sub_reddits = sc.textFile(inputs)
    sub_reddits_json = sub_reddits.map(lambda subreddit: json.loads(subreddit)).cache()
    reddit_score_data = sub_reddits_json.map(map_reddit_score_data)
    reduced_values = reddit_score_data.reduceByKey(add_pairs)
    avg_values = reduced_values.map(get_avg)
    filtered_avg_values = avg_values.filter(lambda sub_reddit: sub_reddit[1] > 0)

    json_sub_reddits = sub_reddits_json.map(map_comments)
    comments_with_avg = json_sub_reddits.join(filtered_avg_values)
    calculated_comments = comments_with_avg.map(calculate_comment_values).sortByKey(ascending=False)
    calculated_comments.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit relative scores')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)