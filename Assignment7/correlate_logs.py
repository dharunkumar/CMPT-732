import sys
import re, math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
logs_schema = types.StructType([
        types.StructField('hostname', types.StringType()),
        types.StructField('datetime', types.StringType()),
        types.StructField('requested_path', types.StringType()),
        types.StructField('bytes_transferred', types.StringType())
    ])


def parse_logs(line):
    try:
        data = line_re.split(line)
        return [element for element in data if len(element) > 0]  # ('slppp6.intermind.net', '01/Aug/1995:00:00:10', '/history/skylab/skylab.html', '1687')
    except Exception as e:
        pass


def main(inputs):
    logs = sc.textFile(inputs)
    parsed_logs = logs.map(parse_logs)
    filtered_logs = parsed_logs.filter(lambda x: len(x) == 4)
    logs_df = spark.createDataFrame(filtered_logs, schema=logs_schema)
    hostnames_df = logs_df.groupby(logs_df['hostname']).\
        agg(functions.count('hostname').alias('request_count'),
            functions.sum('bytes_transferred').alias('sum_request_bytes'))
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
    r = numerator / denominator
    r2 = r ** 2
    print("r: ", r)
    print("r^2: ", r2)


if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('logs correlation').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)