package com.liyuqi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * @author liyuqi
 * @version 1.0
 * @date 2022/5/22 19:59
 * @description:
 */
public class WebsiteKPIPVSubmitter {

    public static class KPI {
        private String remote_addr;// 记录客户端的ip地址
        private String remote_user;// 记录客户端用户名称,忽略属性"-"
        private String time_local;// 记录访问时间与时区
        private String request;// 记录请求的url与http协议
        private String status;// 记录请求状态；成功是200
        private String body_bytes_sent;// 记录发送给客户端文件主体内容大小
        private String http_referer;// 用来记录从那个页面链接访问过来的
        private String http_user_agent;// 记录客户浏览器的相关信息

        private boolean valid = true;// 判断数据是否合法

        @Override
        public String toString() {
            return "valid:" + this.valid +
                    "\nremote_addr:" + this.remote_addr +
                    "\nremote_user:" + this.remote_user +
                    "\ntime_local:" + this.time_local +
                    "\nrequest:" + this.request +
                    "\nstatus:" + this.status +
                    "\nbody_bytes_sent:" + this.body_bytes_sent +
                    "\nhttp_referer:" + this.http_referer +
                    "\nhttp_user_agent:" + this.http_user_agent;
        }

        public String getRemote_addr() {
            return remote_addr;
        }

        public void setRemote_addr(String remote_addr) {
            this.remote_addr = remote_addr;
        }

        public String getRemote_user() {
            return remote_user;
        }

        public void setRemote_user(String remote_user) {
            this.remote_user = remote_user;
        }

        public String getTime_local() {
            return time_local;
        }

        public Date getTime_local_Date() throws ParseException {
            SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.getDefault());
            return df.parse(this.time_local);
        }

        public String getTime_local_Date_hour() throws ParseException {
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
            return df.format(this.getTime_local_Date());
        }

        public void setTime_local(String time_local) {
            this.time_local = time_local;
        }

        public String getRequest() {
            return request;
        }

        public void setRequest(String request) {
            this.request = request;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getBody_bytes_sent() {
            return body_bytes_sent;
        }

        public void setBody_bytes_sent(String body_bytes_sent) {
            this.body_bytes_sent = body_bytes_sent;
        }

        public String getHttp_referer() {
            return http_referer;
        }

        public String getHttp_referer_domain() {
            if (http_referer.length() < 8) {
                return http_referer;
            }

            String str = this.http_referer.replace("\"", "").replace("http://", "").replace("https://", "");
            return str.indexOf("/") > 0 ? str.substring(0, str.indexOf("/")) : str;
        }

        public void setHttp_referer(String http_referer) {
            this.http_referer = http_referer;
        }

        public String getHttp_user_agent() {
            return http_user_agent;
        }

        public void setHttp_user_agent(String http_user_agent) {
            this.http_user_agent = http_user_agent;
        }

        public boolean isValid() {
            return valid;
        }

        public void setValid(boolean valid) {
            this.valid = valid;
        }

        // 将分割的字符串转换成KPI对象
        private static KPI parser(String line) {
            KPI kpi = new KPI();
            // 根据空格字符串分割
            String[] arr = line.split(" ");
            if (arr.length > 11) {
                kpi.setRemote_addr(arr[0]);
                kpi.setRemote_user(arr[1]);
                kpi.setTime_local(arr[3].substring(1));
                kpi.setRequest(arr[6]);
                kpi.setStatus(isNumeric(arr[8]) ? arr[8] : arr[9]);
                kpi.setBody_bytes_sent(arr[9]);
                kpi.setHttp_referer(arr[10]);

                if (arr.length > 12) {
                    kpi.setHttp_user_agent(arr[11] + " " + arr[12]);
                } else {
                    kpi.setHttp_user_agent(arr[11]);
                }

                if (Integer.parseInt(kpi.getStatus()) > 404) {// 大于400，HTTP错误
                    kpi.setValid(false);
                }
            } else {
                kpi.setValid(false);
            }
            return kpi;
        }

        // 判断是否为数字
        public static boolean isNumeric(String str){
            for (int i = str.length();--i>=0;){
                if (!Character.isDigit(str.charAt(i))){
                    return false;
                }
            }
            return true;
        }

        // 过滤不合法的请求路径
        public static KPI filterPVs(String line) {
            KPI kpi = parser(line);
            Set<String> pages = new HashSet<>();
            pages.add("/");
            pages.add("");
            pages.add(" ");
            pages.add(null);

            // 若当前请求地址存在pages这个set集合
            if (pages.contains(kpi.getRequest())) {
                // 将该kpi对象设置为不合法
                kpi.setValid(false);
            }
            return kpi;
        }

    }

    public static class KPIPVMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 将切割的value转换成kpi对象，并且判断是否合法
            KPI kpi = KPI.filterPVs(value.toString());
            if (kpi.isValid()) {
                // 合法则写入context
                word.set(kpi.getRequest());
                context.write(word, one);
            }
        }

    }

    public static class KPIPVReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // 对map传过来的key，进行reduce，数目累加
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            // 并以传入的request为key，计算出的result作为value
            context.write(key, result);
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
        job.setJarByClass(WebsiteKPIPVSubmitter.class);
        //设置最终输出的kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置job所用的mapper逻辑类为哪一个类
        job.setMapperClass(WebsiteKPIPVSubmitter.KPIPVMapper.class);
        job.setCombinerClass(WebsiteKPIPVSubmitter.KPIPVReducer.class);
        //设置job所用的reducer逻辑类为哪一个类
        job.setReducerClass(WebsiteKPIPVSubmitter.KPIPVReducer.class);

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
