import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4'  # make sure we have Spark 2.4+

from colour_tools import colour_schema, rgb2lab_query, plot_predictions
from pyspark.ml.feature import VectorAssembler, StringIndexer, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    #  RGB Model
    rgb_assembler = VectorAssembler(
        inputCols=['R', 'G', 'B'],
        outputCol='features')

    word_indexer = StringIndexer(inputCol="word", outputCol="indexed_targets", handleInvalid="error",
                                  stringOrderType="frequencyDesc")

    classifier = MultilayerPerceptronClassifier(
        layers=[3, 30, 11], featuresCol='features', labelCol='indexed_targets')
    #  create a pipeline to predict RGB colours -> word

    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)
    predictions = rgb_model.transform(validation)
    #  create an evaluator and score the validation data

    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",
                                                  labelCol='indexed_targets')
    score = evaluator.evaluate(predictions)
    plot_predictions(rgb_model, 'RGB', labelCol='word')
    print('Validation score for RGB model: %g' % (score, ))

    #  LAB Model
    rgb_to_lab_query = rgb2lab_query(passthrough_columns=["word"])
    sqlTrans = SQLTransformer(statement=rgb_to_lab_query)

    lab_assembler = VectorAssembler(
        inputCols=['labL', 'labA', 'labB'],
        outputCol='features')

    lab_pipeline = Pipeline(stages=[sqlTrans, lab_assembler, word_indexer, classifier])
    lab_model = lab_pipeline.fit(train)
    lab_predictions = lab_model.transform(validation)

    lab_score = evaluator.evaluate(lab_predictions)
    plot_predictions(lab_model, 'LAB', labelCol='word')
    print('Validation score for LAB model: %g' % (lab_score,))


if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
