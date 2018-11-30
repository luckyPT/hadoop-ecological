package com.pt.hadoop.mr;

import com.pt.hadoop.hdfs.FileSystemOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordCounter {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(FileSystemOperation.class.getResourceAsStream("/hdfs/conf.xml"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCounter.class);
        job.setMapperClass(WordCounterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //自定义partition
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(2);
        //在map端先进行reduce，减少数据吞吐量; 只适合用于reduce的输入和输出的数据类型一致的情况
        job.setCombinerClass(WordCounterReducer.class);

        job.setReducerClass(WordCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //注意不可引用mapred包下面的，会编译报错
        FileInputFormat.setInputPaths(job, new Path("/user/panteng/LICENSE.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/user/panteng/wordCount"));
        //这行必须有
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    static class WordCounterMapper extends Mapper<Object, Text, Text, LongWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            LongWritable one = new LongWritable(1L);
            for (String word : words) {
                context.write(new Text(word), one);
            }
        }
    }

    static class WordCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    /**
     * 如果outputValueClass是自定义对象，改对象需要实现Writable接口
     */
    static class MyObjectWriteable implements Writable {
        @Override
        public void write(DataOutput dataOutput) throws IOException {

        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {

        }
    }

    /**
     * job.setPartitionerClass(MyPartitioner.class);
     * job.setNumReduceTasks(2);
     * 自定义Partitioner
     */
    static class MyPartitioner extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text text, LongWritable longWritable, int i) {
            if (text.toString().length() < 4) {
                return 0;
            } else {
                return 1;
            }
        }
    }
}
