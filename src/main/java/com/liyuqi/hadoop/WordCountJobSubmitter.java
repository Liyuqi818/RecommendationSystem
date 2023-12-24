package com.synear.hadoop;

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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @author liyuqi
 * @version 1.0
 * @date 2022/3/24 15:24
 * @description:
 */
public class WordCountJobSubmitter {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {


        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] ars = value.toString().split("['.;,?| \t\n\r\f]");
            for (int i = 0; i < ars.length; i++) {
                if (ars[i].equals("Watson")) {
                    word.set(ars[i]);
                    context.write(word, one);
                } else if (ars[i].equals("Sherlock")) {
                    word.set(ars[i] + " Holmes");
                    context.write(word, one);
                } else if (ars[i].equals("Jefferson")) {
                    if (ars[i + 1].equals("Hope")) {
                        word.set(ars[i] + " Hope");
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        FileSystem fs = FileSystem.get(conf);
        // 获取Run as -> Args 里面的两个参数
        // 如果没有2个参数，则会报以下错误，用户需要重新添加参数
        if (otherArgs.length < 2) {
            System.err.println("Usage: Need two configuration args!");
            System.exit(2);
        }

        Job wordCountJob = Job.getInstance(conf);
        //重要：指定本job所在的jar包
        wordCountJob.setJarByClass(WordCountJobSubmitter.class);
        //设置wordCountJob所用的mapper逻辑类为哪一个类
        wordCountJob.setMapperClass(TokenizerMapper.class);
        //设置wordCountJob所用的reducer逻辑类为哪一个类
        wordCountJob.setReducerClass(IntSumReducer.class);
        //设置map阶段输出的kv数据类型
        wordCountJob.setMapOutputKeyClass(Text.class);
        wordCountJob.setMapOutputValueClass(IntWritable.class);
        //设置最终输出的kv数据类型
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);
        //设置要处理的文本数据所存放的路径
        // 文件读取目录： hdfs://192.162.11.202:9111/wordcount/input arg0
        // 临时目录： hdfs://192.162.11.202:9111/wordcount/temp arg1
        // 最终结果输出目录： hdfs://192.162.11.202:9111/wordcount/output arg2
        FileInputFormat.setInputPaths(wordCountJob, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(wordCountJob, new Path(otherArgs[1]));
        // 设置输出文件格式
        wordCountJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        //统计完单词 再执行排序任务
        if (wordCountJob.waitForCompletion(true)) {
            Job sortJob = Job.getInstance(conf);
            sortJob.setJarByClass(WordCountJobSubmitter.class);

            FileInputFormat.addInputPath(sortJob, new Path(otherArgs[1]));
            // 设置输入文件格式
            sortJob.setInputFormatClass(SequenceFileInputFormat.class);

            // InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换
            sortJob.setMapperClass(InverseMapper.class);
            // 将 Reducer 的个数限定为1, 最终输出的结果文件就是一个
            sortJob.setNumReduceTasks(1);
            FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[2]));

            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);
            /*Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。
             * 因此我们实现了一个 IntWritableDecreasingComparator 类,
             * 并指定使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行排序*/
            sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
            if (sortJob.waitForCompletion(true)) {
                System.out.println(fs.delete(new Path(otherArgs[1]), true) ? "delete success!" : "delete fail!");
            }
            System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
        }
    }

}


