import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types


def main(inputs, output):
    observation_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])

    weather = spark.read.csv(inputs, schema=observation_schema)
    qflag_filtered_weather = weather.filter(weather['qflag'].isNull())
    tmin_weather_df = qflag_filtered_weather.filter(qflag_filtered_weather['observation'] == 'TMIN').\
        withColumn('tmin', qflag_filtered_weather['value'] / 10)
    # print(tmin_weather_df.show())
    tmax_weather_df = qflag_filtered_weather.filter(qflag_filtered_weather['observation'] == 'TMAX').\
        withColumn('tmax', qflag_filtered_weather['value'] / 10)
    weather_df = tmax_weather_df.join(tmin_weather_df, ['date', 'station'])

    temp_range_df = weather_df.withColumn('temp_range', (weather_df['tmax'] - weather_df['tmin'])).cache()
    max_temp_range_df = temp_range_df.groupby(temp_range_df['date']).max('temp_range')
    max_temp_range_df = max_temp_range_df.select(max_temp_range_df['date'], max_temp_range_df['max(temp_range)'].alias('temp_range'))

    # functions.broadcast(max_temp_range_df)
    result_df = temp_range_df.join(functions.broadcast(max_temp_range_df),
                                   ['date', 'temp_range']
                                   ).select(temp_range_df['date'],
                                            temp_range_df['station'],
                                            temp_range_df['temp_range'].alias('range')
                                            )
    sorted_result_df = result_df.sort([result_df['date'], result_df['station']], ascending=True)
    sorted_result_df.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)