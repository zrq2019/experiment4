## <font color='midblue'>实验四 - 个贷违约预测</font>

### 1 实验背景

​        本次实验以金融风控中的个⼈人信贷为背景，根据贷款申请⼈人的数据信息来预测其是否有违约的可能，从⽽而以此判断是否通过此项贷款。本次实验选取的训练数据是某网络信用贷产品违约记录数据，包含在 train_data.csv 文件中，共有 30 万条。

- 数据描述

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-08 115258.jpg)

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-08 115330.jpg)



### <font color='midblue'>2 任务一：编写 mapreduce 程序计数并排序</font>

​     具体要求：编写 MapReduce 程序，统计每个工作领域 industry 的网贷记录的数量，并按数量从大到小进行排序。

​     输出格式：<工作领域> <记录数量>

#### 2.1 设计思路

- 实现 doMapper 类，使用 StringTokenizer 一行一行读取文件，并以" , "为分隔符，使用 for 循环读取到第 11 个单词，即为 industry；

​       注意：要排除第一行的 header : "industry"，否则会报错

- 实现 doReducer类，对<industry,value>进行合并；
- 实现 IntWritableDecreasingComparator 类，对合并后的<industry,value>进行排序；
- 实现 Write 类，将 output//part-r-00000 文件按照指定格式输出。

#### 2.2 实现细节

- **main 函数**

```Java
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        Path tempDir = new Path("wordcount-temp1-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE))); //定义一个临时目录
        Job job = Job.getInstance(conf, "count");
        job.setJarByClass(count.class);
        try {
            job.setMapperClass(doMapper.class);
            job.setReducerClass(doReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            Path in = new Path("train_data.csv");
            FileOutputFormat.setOutputPath(job, tempDir);//先将词频统计任务的输出结果写到临时目录中, 下一个排序任务以临时目录为输入目录
            FileInputFormat.addInputPath(job, in);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            if (job.waitForCompletion(true)) {
                Job sortJob = Job.getInstance(conf, "sort");
                sortJob.setJarByClass(count.class);
                FileInputFormat.addInputPath(sortJob, tempDir);
                sortJob.setInputFormatClass(SequenceFileInputFormat.class);
                /*InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换*/
                sortJob.setMapperClass(InverseMapper.class);
                /*将 Reducer 的个数限定为1, 最终输出的结果文件就是一个*/
                sortJob.setNumReduceTasks(1);
                FileOutputFormat.setOutputPath(sortJob, new Path("output"));
                sortJob.setOutputKeyClass(IntWritable.class);
                sortJob.setOutputValueClass(Text.class);
                sortJob.setSortComparatorClass(count.IntWritableDecreasingComparator.class);
                if (sortJob.waitForCompletion(true))
                {
                    Write("result");
                } else {
                    System.out.println("1-- not");
                    System.exit(1);
                }
                FileSystem.get(conf).deleteOnExit(tempDir);
                System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
            }
        } catch (IllegalStateException | IllegalArgumentException e) {
            e.printStackTrace();
        }
    }
```

- **doMapper 类**

```java
    public static class doMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        public static final IntWritable one = new IntWritable(1);
        public static Text word = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
            for(int i=0;i<11;i++)
            {
                word.set(tokenizer.nextToken());
            }
            if(Objects.equals(word.toString(), "industry"))
            {
                return;
            }
            context.write(word, one);
        }
    }
```

- **doReducer 类**

```Java
    public static class doReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private final IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable value : values)
            {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
```

- **IntWritableDecreasingComparator 类**

```Java
    private static class IntWritableDecreasingComparator extends IntWritable.Comparator
    {
        public int compare(WritableComparable a, WritableComparable b)
        {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
        {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
```

- **Write 函数**

```Java
public static void Write(String name)
{
    try
    {
        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(conf);
        FSDataOutputStream os=fs.create(new Path(name));
        BufferedReader br=new BufferedReader(new FileReader("output//part-r-00000"));
        String str=null;
        while((str=br.readLine())!=null){
            String[] str2=str.split("\t");
            str=str2[1]+" "+str2[0]+"\n";
            byte[] buff=str.getBytes();
            os.write(buff,0,buff.length);
        }
        br.close();
        os.close();
        fs.close();
    }
    catch(Exception e)
    {
        e.printStackTrace();
    }
}
```

#### 2.3 输出结果

- **结果储存在 task1/result 文件中**

```
金融业 48216
电力、热力生产供应业 36048
公共服务、社会组织 30262
住宿和餐饮业 26954
文化和体育业 24211
信息传输、软件和信息技术服务业 24078
建筑业 20788
房地产业 17990
交通运输、仓储和邮政业 15028
采矿业 14793
农、林、牧、渔业 14758
国际组织 9118
批发和零售业 8892
制造业 8864
```

#### 2.4 问题原因与解决方案

- 输出结果统计个数与实际不符

​       原因：通过观察，发现 industry 每一项计数结果都少于实际值，仔细检查程序未发现问题，通过核对 train_data.csv 文件大小发现在将文件从本地复制到虚拟机的过程中，一部分数据丢失，导致统计个数与实际不符，重新复制文件并运行程序，结果正确。



### <font color='midblue'>3 任务二：编写 spark 程序统计分布情况</font>

​     编写 Spark 程序，统计网络信用贷产品记录数据中所有用户的贷款金金额 total_loan 的分布情况。以 1000 元为区间进⾏行行输出。

​     输出格式示例：((2000,3000),1234)

#### 3.1 设计思路

- 将每一行的 total_loan 根据其值以键值对形式映射到对应的区间范围内
- 使用 reduceByKey(add) 函数统计每个区间上的总数，并以给定形式输出

#### 3.2 实现细节

```Java
# 自定义函数，映射为键值对
def new_map(x):
    // total_loan取值
    s=x.split(',')[2]
    for i in range(41):
        if(1000*i<=float(s)<1000*(i+1)):
            return("["+str(1000*i)+","+str(1000*(i+1))+")",1)
        else:
            print("not here")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(sys.stderr, "Usage: statistics <file>")
        exit(-1)
    sc = SparkContext(appName="PythonStatistics")
    lines = sc.textFile(sys.argv[1], 1)
    counts = lines.flatMap(lambda x: x.split(',')) \
                  .map(new_map) \
                  .reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s%s,%i%s" % ("(",word, count,")"))
    sc.stop()
```

#### 3.3 输出结果

- 左闭右开区间

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-04 212536.jpg)

- **优化：将输出按照区间的大小排序，使用 sortBy() 函数**

```Java
counts = lines.map(new_map) \
              .reduceByKey(add) \
              .sortBy(lambda x: int(x[0].split(',')[0].strip('[')))
```

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-04 213305.jpg)

#### 3.4 问题原因与解决方案

- 原因：未用逗号对 string 分词，添加 s=x.split(',')[2] 后运行成功。

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-04 202618.jpg)

- 原因：循环次数不够，应有 41 个区间，修改区间个数即可。

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-04 205835.jpg)

- 原因：在 for 循环中使用 continue 关键词报错，目前还未搞清楚原因，用 print() 代替 continue 该问题得到解决。

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-04 211103.jpg)

- 原因：对于 python 2.x 版本的输出语句，格式书写错误

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-04 212141.jpg)



### <font color='midblue'>4 任务三：基于 Spark SQL 对数据进行统计</font>

#### 4.1 统计所有用户所在公司类型 employer_type 的数量分布占比情况

​        统计所有用户所在公司类型 employer_type 的数量分布占比情况。输出成 CSV 格式的文件。

​        输出内容格式为：<公司类型>,<类型占比>

##### 4.1.1 设计思路

- 使用 groupBy() 函数对 employer_type 行进行分类加和；
- 使用 spark.sql.functions 中的 col() 函数获取值；
- 使用 cast() 函数将数据类型转换为 float ;
- 使用 coalesce(1) 函数将特定列输出到同一个文件中，不特别指定则将分成多个文件输出

##### 4.1.2 实现细节

```Java
# 方法一：将 df 写入表中，使用 sql 语句计算，再写入 csv 文件中
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: sparksql <file>"
        exit(-1)
    sc = SparkSession.builder.master("local").appName("sparkSQL").getOrCreate()
    df = sc.read.csv('train_data2.csv')
    # 写入表中
    df.createOrReplaceTempView("people")
    sqlDF = sc.sql("SELECT * FROM people limit 10")
            ...
    sqlDF.show()
```

```Java
# 方法二：直接使用 df 进行计算
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: sparksql <file>"
        exit(-1)
    sc = SparkSession.builder.master("local").appName("sparkSQL").getOrCreate()
    df = sc.read.csv('train_data2.csv')
    # 计算数据总条数
    tot = df.count()
    # 计算并输出
    df.groupBy('_c9').count().withColumn('percent', (F.col('count') / tot).cast("Float")).select("_c9", "percent").coalesce(1).write.csv("output",header=False)  
    sc.stop()
```

##### 4.1.3 输出结果

- 输出结果保存在 output1 中

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-05 211342.jpg)

```
幼教与中小学校,0.100910395
上市企业,0.09938881
政府机构,0.2576079
世界五百强,0.05408654
高等教育机构,0.033257466
普通企业,0.45474887
```

##### 4.1.4 问题原因与解决方案

- 原因：在 bdkit 上运行集群存在问题，多尝试几次即可解决

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-05 105951.jpg)

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-05 175945.jpg)

- 输出 dataframe 时，发现会自动生成 header，这是正常现象

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-05 142457.jpg)

- 原因：做除法时没有转换类型

  方案：(F.col('count') / tot).cast("Float") 转换为 float

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-05 205553.jpg)



#### 4.2 统计每个用户最终须缴纳的利息金额

##### 4.2.1 设计思路

- 使用 withColumn 函数增加新的一列
- 使用 F 函数进行公式计算

##### 4.2.2 实现细节

```Java
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: sparksql <file>"
        exit(-1)
    sc = SparkSession.builder.master("local").appName("sparkSQL").getOrCreate()
    df = sc.read.csv('train_data2.csv')
    df.withColumn('total_money', ((F.col('_c3')*F.col('_c5')*12) - F.col('_c2')).cast("Float")).select("_c1", "total_money").coalesce(1).write.csv("output2",header=False)
    sc.stop()
```

##### 4.2.3 输出结果

- 输出结果储存在 output2 中（这里选取前50条数据作为展示）

```
0,3846.0
1,1840.6
2,10465.6
3,1758.52
4,1056.88
5,7234.64
6,757.92
7,4186.96
8,2030.76
9,378.72
10,4066.76
11,1873.56
12,5692.28
13,1258.68
14,6833.6
15,9248.2
16,6197.12
17,1312.44
18,5125.2
19,1215.84
20,1394.92
21,5771.4
22,3202.48
23,4940.6
24,12231.0
25,1070.76
26,9341.8
27,3400.08
28,753.92
29,1564.08
30,1652.44
31,3152.64
32,1784.28
33,10092.16
34,1369.48
35,1955.6
36,586.12
37,8686.4
38,2718.92
39,3179.6
40,2271.48
41,1517.24
42,6558.0
43,2622.04
44,2692.24
45,2302.28
46,2919.72
47,20142.6
48,2159.56
49,1077.04
```

##### 4.2.4 问题原因与解决方案

- 原因：attribute 的使用先后也很重要，改变顺序可能会导致某些报错

  方案：master(“local”) 紧跟在 builder 后

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-08 175519.jpg)



#### 4.3 统计工作年限超过 5 年的用户的房贷情况的数量分布占比情况

​        统计工作年限 work_year 超过 5 年的用户的房贷情况 censor_status 的数量分布占比情况。
输出成 CSV 格式的文件，输出内容格式为：<公司类型>,<类型占比>

##### 4.3.1 设计思路

- 使用 filter() 函数过滤掉 < 1 year 大于 5 years 的情况
- 使用 F.expr() 函数将工作年限的 string 转化成数字

##### 4.3.2 实现细节

```java 
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: sparksql <file>"
        exit(-1)
    sc = SparkSession.builder.master("local").appName("sparkSQL").getOrCreate()
    df = sc.read.csv('train_data2.csv')
    df.filter(df._c11 != '< 1 year').filter(df["_c11"] > "5 years").withColumn('_c11',F.expr("substring(_c11, 1, length(_c11) - 5)")).select("_c1","_c14","_c11").coalesce(1).write.csv("output3",header=False)
    sc.stop()
```

##### 4.3.3 输出结果

- 输出结果储存在 output3 中（这里选取前50条数据作为展示）

```
6	0	8
15	1	7
20	1	7
31	0	6
40	1	6
45	1	6
46	0	8
49	0	6
53	1	7
66	2	7
68	1	6
77	0	8
95	2	6
99	1	6
102	2	6
106	1	9
107	2	8
118	1	6
121	0	8
126	0	8
127	2	6
130	2	6
135	0	8
136	1	6
138	2	7
151	2	6
154	2	8
155	1	9
157	0	7
168	2	7
169	0	6
179	2	8
183	2	9
197	0	8
199	0	8
201	1	9
203	2	9
204	0	7
205	0	7
209	0	6
210	2	6
211	2	8
214	0	8
215	0	7
227	1	6
233	1	6
243	2	8
245	0	9
249	0	8
255	1	9
```

##### 4.3.4 问题原因与解决方案

- 原因：注意到使用 spark 读入的 dataframe 会自动生成新的 header，所以不能用原来的 header 来读数据。

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-05 211932.jpg)



### <font color='midblue'>5 任务四：基于 spark ml 对个人信贷违约进行预测</font>

​        根据给定的数据集，基于 Spark MLlib 或者Spark ML编写程序预测有可能违约的借贷人，并评估实验结果的准确率。数据集中 is_default 反映借贷人是否违约。可按比例把数据集拆分成训练集和测试集，训练模型，并评估模型的性能，评估指标可以是 accuracy、f1 score 等等。

#### 5.1 设计思路

- 数据预处理：因缺失数据较少，我们不采用平均值填充的方法，而选择忽略这些空值；

- 使用 VerctorAssembler 将多个列合并成向量列的特征转换器，即将表中各列用一个类似list表示，输出预测列为单独一列；
- 划分数据集，将训练数据和测试数据分为75%和25%；
- 使用随机森林 RandomForestClassifier 进行数据训练；
- 使用 BinaryClassificationEvaluator,  accuracy_score, f1_score 对二分类评估器的性能进行评估。

#### 5.2 实现细节

```java 
if __name__ == "__main__":

    spark = SparkSession.builder.master("local").appName('random_forest').getOrCreate()

    print('-----------读取并预处理数据------------------')
    # 读取数据
    df = spark.read.csv('train_data2.csv', inferSchema=True, header=True)
    print(df.count(), len(df.columns))
    df.printSchema()

    print('-----------数据转换，将所有的特征值放到一个特征向量中，预测值分开.划分数据用于模型------------------')

    from pyspark.ml.feature import VectorAssembler  # 一个 导入VerctorAssembler 将多个列合并成向量列的特征转换器,即将表中各列用一个类似list表示，输出预测列为单独一列。

    # 映射：将类型为 string 的列值使用 StringIndexer 训练并映射为数字
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
        inputCols=['total_loan', 'year_of_loan', 'interest', 'monthly_payment', 'class2', 'sub_class2',
                   'work_type2', 'employer_type2', 'industry2', 'work_year2', 'house_exist', 'house_loan_status',
                   'censor_status', 'marriage', 'offsprings', 'issue_date2', 'use', 'post_code',
                   'region', 'debt_loan_ratio', 'del_in_18month', 'scoring_low', 'scoring_high',
                   'pub_dero_bankrup', 'early_return', 'early_return_amount', 'early_return_amount_3mon',
                   'recircle_b', 'recircle_u', 'initial_list_status', 'earlies_credit_mon2',
                   'title', 'policy_code', 'f0', 'f1', 'f2', 'f3', 'f4', 'f5'],
        outputCol="features", handleInvalid="keep")
    df = df_assembler.transform(df)
    df.printSchema()

    df.select(['features', 'is_default']).show(10, False)

    model_df = df.select(['features', 'is_default'])  # 选择用于模型训练的数据
    train_df, test_df = model_df.randomSplit([0.75, 0.25])  # 训练数据和测试数据分为75%和25%

    # train_df.groupBy('is_default').count().show()
    # test_num = test_df.groupBy('is_default').count()
    test_num = 75000

    print('-----------使用随机森林进行数据训练----------------')

    from pyspark.ml.classification import RandomForestClassifier

    rf_classifier = RandomForestClassifier(labelCol='is_default', numTrees=50, maxBins=700).fit(
        train_df)  # numTrees设置随机数的数量为50,还有其他参数：maxDepth 树深;返回的模型类型为：RandomForestClassificationModel
    rf_predictions = rf_classifier.transform(test_df)

    print('{}{}'.format('评估每个属性的重要性:', rf_classifier.featureImportances))  # featureImportances : 评估每个功能的重要性,

    rf_predictions.select(['probability', 'is_default', 'prediction']).show(50, False)

    # 查看预测is_default为1的行
    rf_predictions.where("prediction = 1").show()

    print("------查阅pyspark api，没有发现有训练准确率的字段，所以还需要计算预测的准确率------")
        
    # 评估一
    from pyspark.ml.evaluation import BinaryClassificationEvaluator  # 对二进制分类的评估器,它期望两个输入列:原始预测值和标签

    rf_auc = BinaryClassificationEvaluator(labelCol='is_default').evaluate(rf_predictions)
    print(rf_auc)
    print('BinaryClassificationEvaluator 随机森林测试的准确性： {0:.0%}'.format(rf_auc))

    # 评估二
    from sklearn import metrics
        
    # 将预测结果转为python中的dataframe
    columns = rf_predictions.columns  # 提取强表字段
    predictResult = rf_predictions.take(test_num)
    predictResult = pd.DataFrame(predictResult, columns=columns)   # 转为python中的dataframe
    # 性能评估
    y = list(predictResult['is_default'])
    # print(y)
    y_pred = list(predictResult['prediction'])
    # print(y_pred)
    y_predprob = [x[1] for x in list(predictResult['probability'])]

    accuracy_score = metrics.accuracy_score(y, y_pred)  # 准确率
    f1_score = metrics.f1_score(y, y_pred, "binary")    # F1 score

    print("准确率:", accuracy_score) 
    print("F1分数:", f1_score)
        
    spark.stop()
```

#### 5.3 性能评估

- **BinaryClassificationEvaluator  二进制分类的评估器**

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-09 092440.jpg)

- **sklearn.metrics 评估函数**

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-09 092501.jpg)

- **预测 is_default 为 1 的情况**

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-09 092826.jpg)

​        可以看出，用两种方法计算出的 accuracy_score 相对来说是令人满意的，都在 0.8 以上，但是 F1 score 的分数比较低。考虑到 F1 score 是精确率和召回率的调和平均数，猜测该模型的召回率较低，经计算 recall_score，确实如此，进一步考虑调参来提高模型的 F1 score。



#### 5.4 问题原因与解决方案

- 原因：集群运行出现问题，numpy 莫名报错，实际上没有用到

​       方案：在本地 pycharm 环境运行（只需安装 pyspark 即可）

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-06 101508.jpg)



- 原因：java 版本过高

  方案：安装 java 8，手动指定 java 版本

  ![](C:\Users\THINK\Desktop\屏幕截图 2021-12-06 222012.jpg)

  ```Java
  import os
  
  java_location = "C:\Program Files\Java\jdk1.8.0_301"  # 设置你自己的路径
  os.environ['JAVA_HOME'] = java_location
  ```



- 原因：VectorAssembler() 函数不能接受 string 类型的输入

  方案：将类型为 string 的值映射为数字

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-07 152112.jpg)

```Java
    # 映射
    from pyspark.ml.feature import StringIndexer
    # 创建StringIndexer对象，设定输入输出参数
    indexer1 = StringIndexer(inputCol='class', outputCol='class2')
    # 对这个DataFrame进行训练
    model1 = indexer1.fit(df).setHandleInvalid("keep")
    # 利用生成的模型对DataFrame进行transform操作
    df = model1.transform(df)
```



- 原因：maxBins 小于小于某个类别的 value 个数

  方案：调大 maxBins 使其大于等于类别的 value 个数

![](C:\Users\THINK\Desktop\屏幕截图 2021-12-07 164729.jpg)

```Java
    rf_classifier = RandomForestClassifier(labelCol='is_default', numTrees=50, maxBins=700).fit(
        train_df)  # numTrees设置随机数的数量为50,还有其他参数：maxDepth 树深;返回的模型类型为：RandomForestClassificationModel
    rf_predictions = rf_classifier.transform(test_df)
```



### <font color='midblue'>6 心得体会</font>

- 这次实验完美地融合了我们在大数据课上学到的大部分技术，第一次应用 spark 到实际的问题中，并尝试了用随机森林进行监督学习来预测个贷违约情况；
- 在这个过程中，我意识到还有很多有趣的知识技术等待我们去学习，随着科技的发展，大数据处理技术在生活中会越来越重要。
