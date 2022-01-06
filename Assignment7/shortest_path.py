import os
import sys

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

graph_schema = types.StructType([
        types.StructField('source', types.StringType()),
        types.StructField('destination', types.ArrayType(types.StringType()))
    ])

known_paths_schema = types.StructType([
  types.StructField('node', types.StringType()),
  types.StructField('source', types.StringType()),
  types.StructField('distance', types.IntegerType())
  ])


def build_graph_edges(link):
    data = link.split(":")
    node = data[0]
    edges = data[1].strip().split(" ")
    return node, edges


def construct(graph_df, source, distance):
    node = graph_df.filter(graph_df['source'] == source)
    rdd = sc.parallelize([])
    result_df = spark.createDataFrame(rdd, schema=known_paths_schema)
    destination = node.select(node['destination']).collect()[0]
    for n in destination[0]:
        df = create_known_path(n, source, distance+1)
        result_df = result_df.union(df)
    return result_df


def create_known_path(node, source, distance):
    df = spark.createDataFrame(
        [
            (node, source, distance)
        ],
        schema=known_paths_schema
    )
    return df


def main(inputs, output, source, destination):
    filepath = inputs + os.path.sep + "links-simple-sorted.txt"
    links = sc.textFile(filepath)
    graph = links.map(build_graph_edges)
    graph_df = spark.createDataFrame(graph, schema=graph_schema).cache()
    distance = 0
    known_paths_df = create_known_path(source, "-", distance)
    for i in range(6):
        distance_df = known_paths_df.filter(known_paths_df['distance'] == distance)
        distance_n_nodes = distance_df.select(distance_df['node']).collect()[0]
        for source_node in distance_n_nodes[0]:
            iteration_df = construct(graph_df, source_node, distance)
            known_paths_df = known_paths_df.union(iteration_df).cache()
            known_paths_df.write.format("csv").option("header", "true").mode("overwrite").save(output + os.path.sep + 'iter-' + str(i))
        distance += 1
        current_nodes = known_paths_df.select(functions.collect_list('node')).first()[0]
        if destination in current_nodes:
            break

    shortest_paths_df = known_paths_df.groupby(known_paths_df['node']).min('distance')
    shortest_paths_df = shortest_paths_df.select(shortest_paths_df['node'], shortest_paths_df['min(distance)'].alias('distance'))
    shortest_paths_df = shortest_paths_df.join(known_paths_df, ['node', 'distance']).cache()

    shortest_path = []
    while True:
        current_node = shortest_paths_df.filter(shortest_paths_df['node'] == destination)
        shortest_path.append(destination)
        destination = current_node.select(current_node['source']).collect()[0][0]
        if destination == source:
            shortest_path.append(destination)
            break
    shortest_path = list(reversed(shortest_path))
    print("shortest_path")
    print(shortest_path)
    sc.parallelize(shortest_path).saveAsTextFile(output + os.path.sep + 'path')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output, source, destination)