import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('ML Pipeline').getOrCreate()
assert spark.version >= '3.0'
spark.sparkContext.setLogLevel('WARN')


from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator


def main():
    data = spark.range(100000)
    data = data.select(
        (functions.rand()*100).alias('length'),
        (functions.rand()*100).alias('width'),
        (functions.rand()*100).alias('height'),
    )
    data = data.withColumn('volume', data['length']*data['width']*data['height'])
    
    training, validation = data.randomSplit([0.75, 0.25])
    
    assemble_features = VectorAssembler(
        inputCols=['length', 'width', 'height'],
        outputCol='features')
    regressor = GBTRegressor(
        featuresCol='features', labelCol='volume')
    pipeline = Pipeline(stages=[assemble_features, regressor])
    
    model = pipeline.fit(training)
    predictions = model.transform(validation)
    predictions.show()
    
    r2_evaluator = RegressionEvaluator(
        predictionCol='prediction', labelCol='volume',
        metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    print(r2)
    

if __name__ == '__main__':
    main()
