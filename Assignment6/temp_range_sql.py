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
    weather.createOrReplaceTempView('weather')
    tmin_weather = spark.sql("""select date, station, (value/10) as tmin from weather 
                                where qflag is NULL and observation == 'TMIN'
                             """)
    tmin_weather.createOrReplaceTempView('tmin_weather')
    tmax_weather = spark.sql("""select date, station, (value/10) as tmax from weather 
                                where qflag is NULL and observation == 'TMAX'
                             """)
    tmax_weather.createOrReplaceTempView('tmax_weather')
    weather_sql_df = spark.sql("""Select tmin_weather.date, tmin_weather.station, tmin_weather.tmin, tmax_weather.tmax, (tmax_weather.tmax-tmin_weather.tmin) as temp_range from tmin_weather 
                                  inner join tmax_weather 
                                  on tmin_weather.date = tmax_weather.date and tmin_weather.station = tmax_weather.station
                               """)
    weather_sql_df.createOrReplaceTempView('weather_sql_df')
    max_temp_range_sql = spark.sql("""Select date, max(temp_range) as temp_range from weather_sql_df
                                      group by date
                                  """)
    max_temp_range_sql.createOrReplaceTempView('max_temp_range_sql')
    result_sql_df = spark.sql("""Select weather_sql_df.date, weather_sql_df.station, weather_sql_df.temp_range as range from weather_sql_df
                                 inner join max_temp_range_sql
                                 on weather_sql_df.date = max_temp_range_sql.date and weather_sql_df.temp_range = max_temp_range_sql.temp_range
                                 order by weather_sql_df.date, weather_sql_df.station
                              """)
    result_sql_df.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)