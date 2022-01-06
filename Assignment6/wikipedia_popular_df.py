import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

wikipedia_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('view_count', types.IntegerType()),
    types.StructField('bytes_returned', types.IntegerType())
])


@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    filename = path.split('/')[-1]
    date = filename.split('-')[1:]
    date[1] = date[1][:2]
    return '-'.join(date)


def main(inputs, output):
    data = spark.read.csv(inputs, schema=wikipedia_schema, sep=' ').withColumn('filename', functions.input_file_name())
    filtered_df = data.filter((data['language'] == 'en') & (data['title'] != 'Main_Page') &
                              (data['title'].startswith('Special:') == False))
    df_with_time = filtered_df.withColumn('timestamp', path_to_hour(data['filename'])).cache()
    largest_pageviews_df = df_with_time.groupBy(df_with_time['timestamp']).max('view_count')
    result_df = df_with_time.join(functions.broadcast(largest_pageviews_df),
                                  [df_with_time['timestamp'] == largest_pageviews_df['timestamp'],
                                   df_with_time['view_count'] == largest_pageviews_df['max(view_count)']]
                                  ).select(df_with_time['timestamp'].alias('hour'),
                                           df_with_time['title'],
                                           df_with_time['view_count'].alias('views')
                                           )
    sorted_result_df = result_df.sort([result_df['hour'], result_df['title']], ascending=True)
    sorted_result_df.write.json(output, mode='overwrite')
    sorted_result_df.explain()


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)