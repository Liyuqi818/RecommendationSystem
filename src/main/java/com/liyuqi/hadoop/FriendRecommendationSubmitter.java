package com.liyuqi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

/**
 * @author 黎雨琪 方鹏菲 邓皓 蔡雨柔
 * @version 1.0
 * @date 2022/5/6 15:09
 * @description:
 */
public class FriendRecommendationSubmitter {

    // 继承Mapper类 重写map方法实现逻辑
    public static class FriendRecommendationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        // 这里为了方便就不去新建实体类作为载体去做了，利用字符串分割实现也可以
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 以tab缩进"\t"为分割符对导入的数据进行分割成字符串数组
            String[] val = value.toString().split("\t");
            // val[0] 代表要进行分析的第一个人  举例： 0  1,2,3,4   val[0]就代表0这个人  而val[1]代表0的朋友们
            long person = Long.parseLong(val[0]);
            List<Long> friends = new ArrayList<>();
            if (val.length == 2) {  // 若等于2 则说明数据分割正常
                // 列出所有直接的好友关系
                for (String friend : val[1].split(",")) {
                    // 将分割出来的元素遍历加入List中
                    friends.add(Long.parseLong(friend));
                    // 生成直接好友键值对 举例： 样例数据:0	1,2,3,4,5,6,7   则处理后: (0,[1,-1]) (0,[2,-1])  (0[3,-1]) ...
                    Text temp = new Text(friend + "," + Long.MIN_VALUE);
                    context.write(new LongWritable(person), temp);
                }

                // 列出所有可能的好友关系
                for (int i = 0; i < friends.size(); i++) {
                    for (int j = i + 1; j < friends.size(); j++) {
                        // [2, 0] [3, 0] [4 0]...|| [3, 0] [4, 0] [5, 0] || (以此类推)...
                        Text oneWay = new Text(friends.get(j) + "," + person);
                        //(1,[2,0]) (1,[3,0]) (1,[4,0])...|| (2,[3,0]) (2,[4,0]) (2,[5,0])... || (以此类推)...
                        context.write(new LongWritable(friends.get(i)), oneWay);

                        // [1, 0] || [2, 0] || [3, 0] || (以此类推)...
                        Text otherWay = new Text(friends.get(i) + "," + person);
                        // (2, [1,0]) (3, [1,0]) (4, [1,0])... || (3, [2,0]) (4, [2,0]) (5, [2,0])... || (以此类推)...
                        context.write(new LongWritable(friends.get(j)), otherWay);
                    }
                }
            }
        }
    }

    // 继承Reducer类 重写reduce方法实现逻辑
    public static class FriendRecommendationReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 用于存储经过上面map方法处理后，具有相互好友关系的键值对
            final Map<Long, List<Long>> mutual = new HashMap<>();

            for (Text val : values) {
                // 字符串切割成数组 0下标的为个人  1下标的为朋友
                final Long person = Long.parseLong(val.toString().split(",")[0]);
                final Long friend = Long.parseLong(val.toString().split(",")[1]);
                final boolean isFriend = friend.equals(Long.MIN_VALUE);

                if (mutual.containsKey(person)) { // 判断map容器中是否已存在该人，true 进入以下逻辑
                    if (isFriend) { // true 则表示person 和 friend 本身就是存在朋友关系
                        mutual.put(person, null);
                    } else if (mutual.get(person) != null) {
                        mutual.get(person).add(friend); // 添加可能的好友到对应的person的List中
                    }
                } else { //false 进入以下逻辑
                    if (isFriend) {
                        mutual.put(person, null);
                    } else {
                        List<Long> temp = new ArrayList<>();
                        temp.add(friend);
                        mutual.put(person, temp);
                    }
                }
            }

            // 定义比较器 推荐好友按数量倒序排序
            Comparator<Long> sortedComp = new Comparator<Long>() {
                @Override
                public int compare(Long key1, Long key2) {
                    int size1 = mutual.get(key1).size();
                    int size2 = mutual.get(key2).size();
                    if (size1 > size2) {
                        return -1;
                    }
                    else if (size1 == size2 && key1 < key2) {
                        return -1;
                    } else {
                        return 1;
                    }
                }
            };

            SortedMap<Long, List<Long>> sortedMutual = new TreeMap<>(sortedComp);

            // 将mutual容器里的元素put到SortMap里，并以比较器的逻辑进行排序存储
            for (java.util.Map.Entry<Long, List<Long>> entry : mutual.entrySet()) {
                if (entry.getValue() != null) {
                    sortedMutual.put(entry.getKey(), entry.getValue());
                }
            }

            int i = 0;
            // 构建数据输出格式
            StringBuilder output = new StringBuilder();
            for (Map.Entry<Long, List<Long>> entry : sortedMutual.entrySet()) {
                if (i == 0) { // 第一个元素单独处理
                    output.append(entry.getKey().toString()).append(" (").append(entry.getValue().size()).append(": ").append(entry.getValue()).append(")");
                } else { // 其余元素以逗号分隔
                    output.append(",").append(entry.getKey().toString()).append(" (").append(entry.getValue().size()).append(": ").append(entry.getValue()).append(")");
                }
                ++i;
            }
            // 数据格式示例： 0	38737 (5: [82, 41, 12, 3, 1]),18591 (4: [32, 31, 92, 88])...
            context.write(key, new Text(output.toString()));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // 获取Run as -> Args 里面的两个参数
        // 如果没有2个参数，则会报以下错误，用户需要重新添加参数
        if (otherArgs.length < 2) {
            System.err.println("Usage: Need two configuration args!");
            System.exit(2);
        }

        Job job = Job.getInstance(conf);
        //重要：指定本job所在的jar包
        job.setJarByClass(FriendRecommendationSubmitter.class);
        //设置最终输出的kv数据类型
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        //设置job所用的mapper逻辑类为哪一个类
        job.setMapperClass(FriendRecommendationMapper.class);
        //设置job所用的reducer逻辑类为哪一个类
        job.setReducerClass(FriendRecommendationReducer.class);

        // 设置输入文件格式
        job.setInputFormatClass(TextInputFormat.class);
        // 设置输出文件格式
        job.setOutputFormatClass(TextOutputFormat.class);

        // 获取HDFS文件操作客户端
        FileSystem outFs = new Path(args[1]).getFileSystem(conf);
        // 删除output文件夹
        outFs.delete(new Path(args[1]), true);

        //设置要处理的文本数据所存放的路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //设置输出文本数据所存放的路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
