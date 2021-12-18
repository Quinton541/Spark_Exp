import pyspark.sql.types as typ
import pyspark.ml.classification as cl
import pyspark.ml.evaluation as ev
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark import SparkContext
from pyspark.sql import SQLContext
if __name__ == '__main__':
    sc = SparkContext.getOrCreate()
    spark = SQLContext(sc)
    spark_df = spark.read.format("csv").option('header', 'true').option('inferScheme', 'true').load("train_data.csv")
    #删除无关数据
    spark_df = spark_df.drop('loan_id').drop('user_id')
    spark_df = spark_df.fillna("0")
    #将长得像string的类型变量通过映射转换为Double
    StrLabel=["class","sub_class","work_type","issue_date","employer_type","industry","earlies_credit_mon","work_year"]
    for item in StrLabel:
        indexer=StringIndexer(inputCol=item,outputCol="%sIndex"%item)
        spark_df=indexer.fit(spark_df).transform(spark_df)
        spark_df=spark_df.drop(item)
    #将长得像Double但实际上还是String的变量转换为Double
    for item in spark_df.columns:
        spark_df=spark_df.withColumn(item,spark_df[item].cast(typ.DoubleType()))
    #将除了is_default的features列转成向量,并加入df中
    cols=spark_df.columns
    cols.remove("is_default")
    ass = VectorAssembler(inputCols=cols, outputCol="features")
    spark_df=ass.transform(spark_df)
    #选取features与label形成清晰的feature-label模型
    model_df = spark_df.select(["features","is_default"])
    #按8：2的比例，随机数种子541 分割训练集与测试集
    train_df,test_df=model_df.randomSplit([0.8,0.2],seed=541)
    #随机森林
    rf = cl.RandomForestClassifier(labelCol="is_default", numTrees=128, maxDepth=9).fit(train_df)
    res = rf.transform(test_df)
    rf_auc = ev.BinaryClassificationEvaluator(labelCol="is_default").evaluate(res)
    print("随机森林预测准确率为：%f"%rf_auc)
    #逻辑回归
    log_reg = cl.LogisticRegression(labelCol='is_default').fit(train_df)
    res = log_reg.transform(test_df)
    log_reg_auc=ev.BinaryClassificationEvaluator(labelCol="is_default").evaluate(res)
    print("逻辑回归预测准确率为：%f" % log_reg_auc)
    #决策树
    DTC=cl.DecisionTreeClassifier(labelCol='is_default').fit(train_df)
    res=DTC.transform(test_df)
    DTC_auc=ev.BinaryClassificationEvaluator(labelCol="is_default").evaluate(res)
    print("决策树预测准确率为：%f" % DTC_auc)
    #支持向量机
    SVM=cl.LinearSVC(labelCol='is_default').fit(train_df)
    res=SVM.transform(test_df)
    SVM_auc=ev.BinaryClassificationEvaluator(labelCol="is_default").evaluate(res)
    print("支持向量机预测准确率为：%f" % SVM_auc)