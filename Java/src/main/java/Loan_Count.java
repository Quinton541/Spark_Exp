import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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


public class Loan_Count {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        //实现map()函数
        public void map(Object key, Text value, Context context)throws IOException,InterruptedException {
            String word = value.toString();
            context.write(new Text(word), new IntWritable(1));
        }
    }
    public static class IntSumReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();//实现reduce()函数
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;//遍历迭代values，得到同一key的所有value
            for (IntWritable val : values) { sum += val.get(); }
            result.set(sum);//产生输出对<key, value>
            context.write(key, result);
        }
    }


    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            //System.out.println("ss");
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            //System.out.println("ss1");
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        job.setJobName("Loan_Count");
        job.setJarByClass(Loan_Count.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        Path outPath = new Path("/user/quinton_541/tempDir");
        FileInputFormat.addInputPath(job, new Path("/user/quinton_541/input/"));
        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
        Job sortjob = Job.getInstance(config);
        FileInputFormat.addInputPath(sortjob, outPath);
        sortjob.setOutputKeyClass(IntWritable.class);
        sortjob.setOutputValueClass(Text.class);
        sortjob.setInputFormatClass(SequenceFileInputFormat.class);
        sortjob.setMapperClass(InverseMapper.class);
        sortjob.setNumReduceTasks(1);
        sortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);
        outPath= new Path("/user/quinton_541/Mission1_Output");
        FileOutputFormat.setOutputPath(sortjob,outPath);
        sortjob.waitForCompletion(true);
    }
}
