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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
/*
yarn jar first-1.0-SNAPSHOT.jar com.briup.pro_recommend.Step8 -D input=/user/hdfs/shop
hdfs dfs -cat /user/hdfs/shop_result/step8/part-r-00000
 */
public class Step8 extends Configured implements Tool {
    static class logMapper extends Mapper<LongWritable, Text,Text, DoubleWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs=value.toString().split(",");
            if ("paySuccess".equals(strs[2].trim())){
                context.write(new Text(strs[0]+":"+strs[1]),new DoubleWritable(1));
            }
        }
    }
    //
    static class Step8Mapper extends Mapper<Text,DoubleWritable,Text,DoubleWritable>{
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }
    static class Step8Reducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<DoubleWritable> iter=values.iterator();
            double val=iter.next().get();
            if (!iter.hasNext()){
                context.write(key,new DoubleWritable(val));
            }

        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        String input=conf.get("input");
        Path step7_path=new Path("/user/hdfs/shop_result/step7");
        Path step8_path=new Path("/user/hdfs/shop_result/step8");
        FileSystem fs=FileSystem.get(conf);
        if (fs.exists(step8_path)) {
            fs.delete(step8_path,true);
        }

        Job job8=Job.getInstance();
        job8.setJobName("step8");
        job8.setJarByClass(this.getClass());

        //第一个map和原始文件对应
        MultipleInputs.addInputPath(job8,new Path(input), TextInputFormat.class,logMapper.class);

        //读取第七步结果
        MultipleInputs.addInputPath(job8,step7_path, SequenceFileInputFormat.class,Step8Mapper.class);
        job8.setMapOutputKeyClass(Text.class);
        job8.setMapOutputValueClass(DoubleWritable.class);

        job8.setReducerClass(Step8Reducer.class);
        job8.setOutputKeyClass(Text.class);
        job8.setOutputValueClass(DoubleWritable.class);


        job8.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job8,step8_path);
        //TextOutputFormat.setOutputPath(job8,step8_path);
        return job8.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Step8(),args));
    }
}
