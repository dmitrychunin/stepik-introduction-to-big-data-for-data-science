import io
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff
from pyspark.sql import functions as F


def aggregate_by_ad(df):
    cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
    # сгруппируем события по объявлениям
    return df.groupBy('ad_id', 'ad_cost_type').agg(
        F.first('target_audience_count').alias('target_audience_count'),
        F.first('has_video').alias('has_video'),
        F.first('ad_cost').alias('ad_cost'),
        # число дней между первым и последним событием объявления
        datediff(F.max('date'), F.min('date')).alias('day_count'),
        # соотношение количества кликов к показам (CTR)
        (cnt_cond(F.col('event') == 'click') / cnt_cond(F.col('event') == 'view')).alias('CTR'),
        # get dummies из поля ad_cost_type для значения CPM
        (F.first('ad_cost_type') == 'CPM').astype('int').alias('is_cpm'),
        # get dummies из поля ad_cost_type для значения CPC
        (F.first('ad_cost_type') == 'CPC').astype('int').alias('is_cpc'),
    )


def select_dataframe_columns_in_right_order(df):
    return df.select('ad_id', 'target_audience_count', 'has_video', 'is_cpm', 'is_cpc', 'ad_cost', 'day_count', 'CTR')


def process(spark, input_file, target_path):
    input_df = spark.read.parquet(input_file)
    # рассчитаем недостающие колонки
    aggregated_df = aggregate_by_ad(input_df)
    # выберем в нужном порядке только нужные колонки
    output_df = select_dataframe_columns_in_right_order(aggregated_df)
    # разделим датафрейм на три части с соотношением 2/1/1
    train, test, validate = output_df.randomSplit([0.5, 0.25, 0.25], seed=42)

    # сохраним три части в разные папки
    full_target_path = os.path.abspath(os.getcwd()) + '/' + target_path
    print("Save parquet result into path: " + full_target_path)
    train.write.mode('overwrite').parquet(full_target_path + '/train')
    test.write.mode('overwrite').parquet(full_target_path + '/test')
    validate.write.mode('overwrite').parquet(full_target_path + '/validate')


def main(argv):
    input_path = argv[0]
    print("Input path to file: " + input_path)
    target_path = argv[1]
    print("Target path: " + target_path)
    spark = _spark_session()
    process(spark, input_path, target_path)


def _spark_session():
    return SparkSession.builder.appName('PySparkJob').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Input and Target path are require.")
    else:
        main(arg)
