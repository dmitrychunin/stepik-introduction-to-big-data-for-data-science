import io
import sys
import os

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession


LR_MODEL = 'lr_model'


def process(spark, input_file, output_file):
    #input_file - путь к файлу с данными для которых нужно предсказать ctr
    #output_file - путь по которому нужно сохранить файл с результатами [ads_id, prediction]
    bestModel = PipelineModel.load('bestModel')
    test_data = spark.read.parquet(input_file)
    prediction = bestModel.transform(test_data)
    full_target_path = os.path.abspath(os.getcwd()) + '/' + output_file
    prediction.select("ad_id", "prediction").write.csv(full_target_path)


def main(argv):
    input_path = argv[0]
    print("Input path to file: " + input_path)
    output_file = argv[1]
    print("Output path to file: " + output_file)
    spark = _spark_session()
    process(spark, input_path, output_file)


def _spark_session():
    return SparkSession.builder.appName('PySparkMLPredict').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Input and Target path are require.")
    else:
        main(arg)
