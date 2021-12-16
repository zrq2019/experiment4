import sys
from pyspark.sql import Row
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pandas as pd
import os

from sklearn import metrics

java_location = "C:\Program Files\Java\jdk1.8.0_301"  # 设置你自己的路径
os.environ['JAVA_HOME'] = java_location

import sys
from operator import add
import time
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession

# 任务四
if __name__ == "__main__":

    spark = SparkSession.builder.master("local").appName('random_forest').getOrCreate()

    print('-----------读取用于测得数据,检测数据质量和相关的特征。即相对数据有一定的认识，对后续进行逻辑回归训练做准备------------------')
    # 读取数据
    df = spark.read.csv('train_data2.csv', inferSchema=True, header=True)
    # print(df.count(), len(df.columns))
    # df.printSchema()

    print('-----------数据转换，将所有的特征值放到一个特征向量中，预测值分开.划分数据用于模型------------------')

    from pyspark.ml.feature import \
        VectorAssembler  # 一个 导入VerctorAssembler 将多个列合并成向量列的特征转换器,即将表中各列用一个类似list表示，输出预测列为单独一列。

    # 映射
    from pyspark.ml.feature import StringIndexer
    # 创建StringIndexer对象，设定输入输出参数
    indexer1 = StringIndexer(inputCol='class', outputCol='class2')
    # 对这个DataFrame进行训练
    model1 = indexer1.fit(df).setHandleInvalid("keep")
    # 利用生成的模型对DataFrame进行transform操作
    df = model1.transform(df)
    indexer2 = StringIndexer(inputCol='sub_class', outputCol='sub_class2')
    model2 = indexer2.fit(df).setHandleInvalid("keep")
    df = model2.transform(df)
    indexer3 = StringIndexer(inputCol='work_type', outputCol='work_type2')
    model3 = indexer3.fit(df).setHandleInvalid("keep")
    df = model3.transform(df)
    indexer4 = StringIndexer(inputCol='employer_type', outputCol='employer_type2')
    model4 = indexer4.fit(df).setHandleInvalid("keep")
    df = model4.transform(df)
    indexer5 = StringIndexer(inputCol='industry', outputCol='industry2')
    model5 = indexer5.fit(df).setHandleInvalid("keep")
    df = model5.transform(df)
    indexer6 = StringIndexer(inputCol='work_year', outputCol='work_year2')
    model6 = indexer6.fit(df).setHandleInvalid("keep")
    df = model6.transform(df)
    indexer7 = StringIndexer(inputCol='issue_date', outputCol='issue_date2')
    model7 = indexer7.fit(df).setHandleInvalid("keep")
    df = model7.transform(df)
    indexer8 = StringIndexer(inputCol='earlies_credit_mon', outputCol='earlies_credit_mon2')
    model8 = indexer8.fit(df).setHandleInvalid("keep")
    df = model8.transform(df)

    # df.select(['class2','sub_class2','work_type2']).show(10, False)

    df_assembler = VectorAssembler(
        # inputCols=['total_loan', 'year_of_loan', 'interest', 'monthly_payment', 'class2', 'sub_class2',
        #            'work_type2', 'employer_type2', 'industry2', 'work_year2', 'house_exist', 'house_loan_status',
        #            'censor_status', 'marriage', 'offsprings', 'issue_date2', 'use', 'post_code',
        #            'region', 'debt_loan_ratio', 'del_in_18month', 'scoring_low', 'scoring_high',
        #            'pub_dero_bankrup', 'early_return', 'early_return_amount', 'early_return_amount_3mon',
        #            'recircle_b', 'recircle_u', 'initial_list_status', 'earlies_credit_mon2',
        #            'title', 'policy_code', 'f0', 'f1', 'f2', 'f3', 'f4', 'f5'],
        # outputCol="features", handleInvalid="keep")
        inputCols=['total_loan', 'year_of_loan', 'interest', 'monthly_payment',
                   'use', 'post_code',
                   'region', 'debt_loan_ratio', 'del_in_18month', 'scoring_low', 'scoring_high',
                   'pub_dero_bankrup', 'early_return', 'early_return_amount', 'early_return_amount_3mon',
                   'recircle_b', 'recircle_u', 'initial_list_status',
                   'title', 'policy_code', 'f0', 'f1', 'f2', 'f3', 'f4', 'f5'],
        outputCol="features", handleInvalid="keep")
    df = df_assembler.transform(df)
    # df.printSchema()

    # df.select(['features', 'is_default']).show(10, False)

    model_df = df.select(['features', 'is_default'])  # 选择用于模型训练的数据
    train_df, test_df = model_df.randomSplit([0.75, 0.25])  # 训练数据和测试数据分为75%和25%

    # train_df.groupBy('is_default').count().show()
    # test_num = test_df.groupBy('is_default').count()
    test_num = 75000

    print('-----------使用随机森林进行数据训练----------------')

    from pyspark.ml.classification import RandomForestClassifier

    # , maxBins=687
    rf_classifier = RandomForestClassifier(labelCol='is_default', numTrees=100, maxDepth=10, maxBins=32).fit(
        train_df)  # numTrees设置随机数的数量为50,还有其他参数：maxDepth 树深;返回的模型类型为：RandomForestClassificationModel
    rf_predictions = rf_classifier.transform(test_df)

    print('{}{}'.format('评估每个属性的重要性:', rf_classifier.featureImportances))  # featureImportances : 评估每个功能的重要性,

    # rf_predictions.select(['probability', 'is_default', 'prediction']).show(50, False)

    # 查看预测is_default为1的行
    rf_predictions.where("prediction = 1").show()

    print("------查阅pyspark api，没有发现有训练准确率的字段，所以还需要计算预测的准确率------")

    from pyspark.ml.evaluation import BinaryClassificationEvaluator  # 对二进制分类的评估器,它期望两个输入列:原始预测值和标签
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator  # 多类分类的评估器,它期望两个输入列:预测和标签

    rf_auc = BinaryClassificationEvaluator(labelCol='is_default').evaluate(rf_predictions)
    print(rf_auc)
    print('BinaryClassificationEvaluator 随机森林测试的准确性： {0:.0%}'.format(rf_auc))

    # 将预测结果转为python中的dataframe
    columns = rf_predictions.columns  # 提取强表字段
    predictResult = rf_predictions.take(test_num)
    predictResult = pd.DataFrame(predictResult, columns=columns)   # 转为python中的dataframe
    # print(predictResult)
    # 性能评估
    y = list(predictResult['is_default'])
    # print(y)
    y_pred = list(predictResult['prediction'])
    # print(y_pred)
    y_predprob = [x[1] for x in list(predictResult['probability'])]
    precision_score = metrics.precision_score(y, y_pred)  # 精确率
    recall_score = metrics.recall_score(y, y_pred)  # 召回率
    accuracy_score = metrics.accuracy_score(y, y_pred)  # 准确率
    f1_score = metrics.f1_score(y, y_pred, "binary")  # F1分数
    f2 = 2*precision_score*recall_score/(precision_score + recall_score)
    auc_score = metrics.roc_auc_score(y, y_predprob)  # auc分数
    print("精确率:", precision_score)  # 精确率
    print("召回率:", recall_score)  # 召回率
    print("准确率:", accuracy_score)  # 准确率
    print("F1分数:", f1_score)  # F1分数
    print("auc分数:", auc_score)  # auc分数

    spark.stop()
