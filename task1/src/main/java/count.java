import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class count
{
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

            Path in = new Path("train_data2.csv");
            FileOutputFormat.setOutputPath(job, tempDir);//先将词频统计任务的输出结果写到临时目录中, 下一个排序任务以临时目录为输入目录
            FileInputFormat.addInputPath(job, in);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            if (job.waitForCompletion(true)) {
                Job sortJob = Job.getInstance(conf, "sort");
                //Job sortJob = new Job(conf, "sort");
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
                // 实现数据对的key和value交换
                // sortJob.setMapperClass(InverseMapper.class);
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
public static void Write(String name)
{
    try
    {
        Configuration conf=new Configuration();
        // conf.set("fs.defaultFS", "hdfs://localhost:9000");
        // conf.set("fs.hdfs.omp", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs=FileSystem.get(conf);
        FSDataOutputStream os=fs.create(new Path(name));
        BufferedReader br=new BufferedReader(new FileReader("output//part-r-00000"));
        String str=null;
        while((str=br.readLine())!=null){
            //System.out.println("hhhhhhhhh  " + str);
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
            // word.set(tokenizer.nextToken());
//            if(Objects.equals(word.toString(), "industry"))
//            {
//                return;
//            }
            context.write(word, one);
        }
    }
    private static class IntWritableDecreasingComparator extends IntWritable.Comparator
    {
        public int compare(WritableComparable a, WritableComparable b)
        {
            //System.out.println("ss");
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
        {
            //System.out.println("ss1");
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
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
}
