package com.briup.pro_recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
/*
yarn jar first-1.0-SNAPSHOT.jar com.briup.pro_recommend.Step4
hdfs dfs -cat /user/hdfs/shop_result/step4/part-r-00000
 */
public class Step4 extends Configured implements Tool {
    static class Step4Mapper extends Mapper<Text,Text,Text, IntWritable>{
        private IntWritable val=new IntWritable(1);
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String k=key.toString();
            String v=value.toString();
            if(k.equals(v))return;
            context.write(new Text(k+":"+v),val);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        Path step3_path = new Path("/user/hdfs/shop_result/step3");
        Path step4_path = new Path("/user/hdfs/shop_result/step4");
        FileSystem fs=FileSystem.get(conf);
        if (fs.exists(step4_path)) {
            fs.delete(step4_path,true);
        }

        Job job4=Job.getInstance(conf);
        job4.setJobName("step4");
        job4.setJarByClass(Step4.class);

        job4.setMapperClass(Step4Mapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(IntWritable.class);

        job4.setReducerClass(IntSumReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);

        job4.setInputFormatClass(SequenceFileInputFormat.class);
        job4.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job4,step3_path);
        SequenceFileOutputFormat.setOutputPath(job4,step4_path);
        //TextOutputFormat.setOutputPath(job4,step4_path);

        return job4.waitForCompletion(true)?0:-1;

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Step4(),args));
    }
}
