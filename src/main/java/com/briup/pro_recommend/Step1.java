package com.briup.pro_recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
/***
计算某个用户对某个商品的偏好值总和
yarn jar first-1.0-SNAPSHOT.jar com.briup.pro_recommend.Step1 -D input=/user/hdfs/shop
hdfs dfs -cat /user/hdfs/shop_result/step1/part-r-00000
 */
public class Step1 extends Configured implements Tool{
    static class Step1Mapper extends Mapper<LongWritable, Text,Text, DoubleWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs=value.toString().split("[,]");
            if (strs.length==4){
                double hoby_value=0.0;
                if ("showProduct".equals(strs[2])){
                    hoby_value=0.05;
                }else if ("addCart".equals(strs[2])){
                    hoby_value=0.15;
                }else if ("createOrder".equals(strs[2])){
                    hoby_value=0.3;
                }else if ("paySucess".equals(strs[2])){
                    hoby_value=0.5;
                }else {
                    hoby_value=0.1;
                }
                context.write(new Text(strs[0]+":"+strs[1]),new DoubleWritable(hoby_value));
            }
        }
    }
    static class Step1Reducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum=0.0;
            for (DoubleWritable val:values){
                sum+=val.get();
            }
            context.write(key,new DoubleWritable(sum));
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        String input=conf.get("input");
        Path step1_path=new Path("/user/hdfs/shop_result/step1");
        FileSystem fs=FileSystem.get(conf);
        if (fs.exists(step1_path)) {
            fs.delete(step1_path,true);
        }
        Job job1=Job.getInstance(conf);
        job1.setJarByClass(Step1.class);
        job1.setJobName("step1");

        job1.setMapperClass(Step1Mapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);

        job1.setReducerClass(Step1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        //job1.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job1,new Path(input));
        SequenceFileOutputFormat.setOutputPath(job1,step1_path);
        //TextOutputFormat.setOutputPath(job1,step1_path);
        return job1.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Step1(),args));
    }
}
