package com.briup.pro_recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
/*
yarn jar first-1.0-SNAPSHOT.jar com.briup.pro_recommend.Step7
hdfs dfs -cat /user/hdfs/shop_result/step7/part-r-00000
 */
public class Step7 extends Configured implements Tool {
    //map原样输出
    static class Step7Mapper extends Mapper<Text,DoubleWritable,Text,DoubleWritable>{
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }
    static class Step7Reducer extends Reducer<Text, DoubleWritable,Text,DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum_remm=0;
            for (DoubleWritable val:values){
                sum_remm+=val.get();
            }
            context.write(key,new DoubleWritable(sum_remm));
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        Path step6_path=new Path("/user/hdfs/shop_result/step6");
        Path step7_path=new Path("/user/hdfs/shop_result/step7");
        FileSystem fs=FileSystem.get(conf);
        if (fs.exists(step7_path)) {
            fs.delete(step7_path,true);
        }

        Job job7=Job.getInstance(conf);
        job7.setJarByClass(Step7.class);
        job7.setJobName("step7");

        job7.setMapperClass(Step7Mapper.class);
        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(DoubleWritable.class);

        job7.setReducerClass(Step7Reducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(DoubleWritable.class);

        job7.setInputFormatClass(SequenceFileInputFormat.class);
        job7.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job7,step6_path);
        SequenceFileOutputFormat.setOutputPath(job7,step7_path);
        //TextOutputFormat.setOutputPath(job7,step7_path);
        return job7.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Step7(),args));
    }
}
