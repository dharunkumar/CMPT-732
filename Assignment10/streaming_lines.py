import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('streaming example').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')


def main():
    lines = spark.readStream.format('text') \
            .option('path', '/tmp/text').load()

    lengths = lines.select(
        functions.length(lines['value']).alias('len')
    )
    counts = lengths.groupBy('len').count()
    
    stream = counts.writeStream.format('console') \
            .outputMode('update').start()
    stream.awaitTermination(600)


if __name__ == '__main__':
    main()