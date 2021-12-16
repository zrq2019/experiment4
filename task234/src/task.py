import sys
from pyspark.sql import Row
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

def new_map(x):
    s=x.split(',')[2]
    for i in range(41):
        if(1000*i<=float(s)<1000*(i+1)):
            return("["+str(1000*i)+","+str(1000*(i+1))+")",1)
        else:
            print("kkkkk")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: sparksql <file>"
        exit(-1)
    # 任务二
    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    counts = lines.map(new_map) \
                  .reduceByKey(add) \
                  .sortBy(lambda x: int(x[0].split(',')[0].strip('[')))
    # counts = lines.flatMap(lambda x: x.split(',')) \
    #               .map(new_map) \
    #               .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s%s,%i%s" % ("(",word, count,")"))
    sc.stop()
    
    # 任务三
    sc = SparkSession.builder.master("local").appName("sparkSQL").getOrCreate()
    df = sc.read.csv('train_data2.csv')
    # (1)
    tot = df.count()
    df.groupBy('_c9').count().withColumn('percent', (F.col('count') / tot).cast("Float")).select("_c9", "percent").coalesce(1).write.csv("output",header=False)    # .option("delimiter", " ")
    # (2)
    df.withColumn('total_money', ((F.col('_c3')*F.col('_c5')*12) - F.col('_c2')).cast("Float")).select("_c1", "total_money").coalesce(1).write.csv("output2",header=False)
    # (3)
    df.filter(df._c11 != '< 1 year').filter(df["_c11"] > "5 years").withColumn('_c11',F.expr("substring(_c11, 1, length(_c11) - 5)")).select("_c1","_c14","_c11").coalesce(1).write.csv("output3",header=False)
    sc.stop()