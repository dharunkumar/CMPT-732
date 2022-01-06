import sys
import re, math
import uuid
from datetime import datetime
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
logs_schema = types.StructType([
        types.StructField('host', types.StringType()),
        types.StructField('id', types.StringType()),
        types.StructField('bytes', types.IntegerType()),
        types.StructField('datetime', types.DateType()),
        types.StructField('path', types.StringType())
    ])


def parse_logs(line):
    try:
        data = line_re.split(line)
        filtered_data = [element for element in data if len(element) > 0]
        if len(filtered_data) >= 4:
            hostname = filtered_data[0]
            # '01/Jul/1995:00:00:01'
            timestamp = datetime.strptime(filtered_data[1], "%d/%b/%Y:%H:%M:%S")
            path = filtered_data[2]
            bytes_transferred = int(filtered_data[3])
            return [hostname, uuid.uuid1(), bytes_transferred, timestamp, path]
    except Exception as e:
        pass


def main(input_dir, keyspace, table_name):
    logs = sc.textFile(input_dir)
    parsed_logs = logs.map(parse_logs)
    filtered_logs = parsed_logs.filter(lambda x: x and len(x) == 5)
    logs_df = spark.createDataFrame(filtered_logs, schema=logs_schema)
    partitioned_df = logs_df.repartition(numPartitions=20)
    partitioned_df.write.format("org.apache.spark.sql.cassandra").mode("overwrite") \
        .option("confirm.truncate", "true").options(table=table_name, keyspace=keyspace).save()


if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Spark Cassandra load logs') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_dir, keyspace, table_name)
