import sys
import math
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types


def main(keyspace, table_name):
    logs_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keyspace).load()
    print(logs_df.show(10))
    hostnames_df = logs_df.groupby(logs_df['host']).\
        agg(functions.count('host').alias('request_count'),
            functions.sum('bytes').alias('sum_request_bytes'))
    values_df = hostnames_df.select((functions.lit(1)).alias('one'),
                                    (hostnames_df['request_count']).alias('x'),
                                    (hostnames_df['request_count'] ** 2).alias('x^2'),
                                    (hostnames_df['sum_request_bytes']).alias('y'),
                                    (hostnames_df['sum_request_bytes'] ** 2).alias('y^2'),
                                    (hostnames_df['request_count'] * hostnames_df['sum_request_bytes']).alias('xy')
                                    )
    py_values = values_df.groupBy().sum().collect()
    print(py_values)
    numerator = (py_values[0][0] * py_values[0][5]) - (py_values[0][1] * py_values[0][3])
    denominator = (math.sqrt((py_values[0][0] * py_values[0][2]) - (py_values[0][1] ** 2)) *
                   math.sqrt((py_values[0][0] * py_values[0][4]) - (py_values[0][3] ** 2)))
    r = round((numerator / denominator), 6)
    r2 = round((r ** 2), 6)
    print("r: ", r)
    print("r^2: ", r2)


if __name__ == '__main__':
    keyspace = sys.argv[1]
    table_name = sys.argv[2]
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Spark Cassandra correlate logs') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(keyspace, table_name)
