from pyspark import SparkContext
from pyspark.sql import SQLContext
import csv
if __name__ == '__main__':
    sc=SparkContext.getOrCreate()
    spark=SQLContext(sc)
    #读取数据至spark的dataframe
    spark_df=spark.read.format("csv").option('header','true').option('inferScheme','true').load("train_data.csv")

    #执行任务1:统计所有用户所在公司类型 employer_type 的数量分布占比情况。
    #读取employer_type为单独数据框
    employer_type=spark_df.select("employer_type")
    #将employer_type转换为rdd，并提取每一行具体对象的值后进行词频统计
    counts=employer_type.rdd.map(lambda x:"{}".format(x.employer_type)).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)
    output=counts.collect()
    sum=0
    for (item,count) in output: sum+=count
    #写入csv
    with open("Misson3.1_Output.csv", "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["公司类型","类型占比"])
        for (item,count) in output:
            writer.writerow([item,count/sum])

    #执行任务2：统计每个用户最终须缴纳的利息金额
    #在spark数据框中选取所需数据至子数据框
    total_money_para=spark_df.select("user_id","year_of_loan","monthly_payment","total_loan")
    #转化为rdd后按公式计算
    res=total_money_para.rdd.map(lambda x:(x.user_id,float(x.year_of_loan)*float(x.monthly_payment)*12-float(x.total_loan)))
    output=res.collect()
    with open("Misson3.2_Output.csv", "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["user_id","total_money"])
        for (user_id,total_money) in output:
            writer.writerow([user_id,total_money])

    #执行任务3：统计⼯工作年限 work_year 超过 5 年的用户的房贷情况 censor_status 的数量分布占比情况
    #在spark数据框中读取所需数据
    parameter=spark_df.select("user_id","censor_status","work_year")
    #work_year 空值填补为"0 year"
    parameter=parameter.fillna("0 year")
    #work year形式为"<1 year","x years"与"10+ year"，按空格分割后提取倒数第二位
    #满足条件的为"10+"与大于5的个位数
    res=parameter.rdd.map(lambda x:(x.user_id,x.censor_status,x.work_year.split(" ")[-2]))\
                                .filter(lambda x:((len(x[2])>1) or (len(x[2])==1 and int(x[2])>5)))
    output=res.collect()
    with open("Misson3.3_Output.csv", "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["user_id", "censor_status","work_year"])
        for (user_id, censor_status, work_year) in output:
            writer.writerow([user_id, censor_status, work_year])


