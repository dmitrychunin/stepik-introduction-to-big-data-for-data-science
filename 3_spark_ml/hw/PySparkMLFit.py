import io
import sys

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml import Transformer

LR_MODEL = 'lr_model'


# class CustomDropper(Transformer):
#     columns = []
#
#     def __init__(self, columns):
#         super(CustomDropper, self).__init__()
#         self.columns = columns
#
#     def _transform(self, dataframe):
#         dataframe.drop(*self.columns)


def process(spark, train_data, test_data):
    # train_data - путь к файлу с данными для обучения и оценки качества модели
    train_df = spark.read.parquet(train_data)
    feature = VectorAssembler(
        inputCols=train_df.columns[1:len(train_df.columns) - 1],
        outputCol='features')

    lr = LinearRegression(labelCol="ctr", featuresCol="features")
    # dropper = CustomDropper(columns=['target_audience_count', 'has_video', 'is_cpm', 'is_cpc', 'ad_cost', 'day_count'])
    pipeline = Pipeline(stages=[feature, lr])

    paramGrid = ParamGridBuilder() \
        .addGrid(lr.maxIter, [30, 40, 50]) \
        .addGrid(lr.regParam, [0.3, 0.4, 0.5]) \
        .addGrid(lr.elasticNetParam, [0.7, 0.8, 0.9]) \
        .build()

    tvs = TrainValidationSplit(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=RegressionEvaluator(labelCol='ctr', predictionCol='prediction', metricName='rmse'),
        trainRatio=0.8)

    model = tvs.fit(train_df)

    model.bestModel.write().overwrite().save('bestModel')

    print('BestModel MaxIter: {}'.format(model.bestModel.stages[-1]._java_obj.getMaxIter()))
    print('BestModel RegParam: {}'.format(model.bestModel.stages[-1]._java_obj.getRegParam()))
    print('BestModel ElasticNetParam: {}'.format(model.bestModel.stages[-1]._java_obj.getElasticNetParam()))
    print("BestModel RMSE: %f" % round(max(model.validationMetrics), 3))


def main(argv):
    train_data = argv[0]
    print("Input path to train data: " + train_data)
    test_data = argv[1]
    print("Input path to test data: " + test_data)
    spark = _spark_session()
    process(spark, train_data, test_data)


def _spark_session():
    return SparkSession.builder.appName('PySparkMLFitJob').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Train and test data are require.")
    else:
        main(arg)
