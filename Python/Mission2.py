from pyspark import SparkContext
if __name__ == '__main__':
    sc=SparkContext.getOrCreate()
    # 读取文件为rdd对象
    lines=sc.textFile('train_data.csv')
    header = lines.first()  # 第一行 print(header)
    # 剔除csv的首行
    lines = lines.filter(lambda row: row != header)
    # 按逗号分割后提取第三个元素：total_loan
    lines=lines.map(lambda line:line.split(',')[2])
    # int((float(x)//1000)*1000) 提取元素所在区间下限，即1541->1000，54541->54000
    counts = lines.map(lambda x: (int((float(x)//1000)*1000), 1)).reduceByKey(lambda a,b:a+b)
    #排序
    counts=counts.sortByKey()
    # 统计好后，将区间下限还原为区间：1000->(1000,2000)
    counts=counts.map(lambda x:("(%i,%i)"%(x[0],x[0]+1000),x[1]))
    output=counts.collect()
    counts.saveAsTextFile("Misson2_Output")
