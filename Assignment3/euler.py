
from pyspark import SparkConf, SparkContext
import sys
import random
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def main(inputs):
    samples = sc.range(0, int(inputs), numSlices=2)
    total_iterations = 0
    batches = 2
    for i in range(1, batches):
        iterations = 0
        random.seed()
        # group your experiments into batches to reduce the overhead
        for j in range(int(samples.count() / batches)):
            sum = 0.0
            while sum < 1:
                sum += random.random()
                iterations += 1
        total_iterations += iterations
    # can also use int(inputs) instead of samples.count()
    print("total_iterations", total_iterations)
    print("samples.count()", samples.count())
    print(total_iterations / samples.count())


if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    main(inputs)