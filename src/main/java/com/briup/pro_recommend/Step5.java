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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
/*
yarn jar first-1.0-SNAPSHOT.jar com.briup.pro_recommend.Step5
hdfs dfs -cat /user/hdfs/shop_result/step5/part-r-00000


 */
public class Step5 extends Configured implements Tool {
    static class Step5Mapper extends Mapper<Text, IntWritable,Text,Text>{
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String[] ks=key.toString().split(":");
            String k=ks[0];
            String v=ks[1]+":"+value.get();
            context.write(new Text(k),new Text(v));
        }
    }
    static class Step5Reducer extends Reducer<Text,Text,Text,Text>{
        private StringBuffer sb=new StringBuffer();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val:values){
                sb.append(val.toString()).append(",");
            }
            sb.setLength(sb.length()-1);
            context.write(key,new Text(sb.toString()));
            sb.setLength(0);
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        Path step4_path = new Path("/user/hdfs/shop_result/step4");
        Path step5_path = new Path("/user/hdfs/shop_result/step5");
        FileSystem fs=FileSystem.get(conf);
        if (fs.exists(step5_path)) {
            fs.delete(step5_path,true);
        }
        Job job5=Job.getInstance(conf);
        job5.setJarByClass(Step5.class);
        job5.setJobName("step5");

        job5.setMapperClass(Step5Mapper.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);

        job5.setReducerClass(Step5Reducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        job5.setInputFormatClass(SequenceFileInputFormat.class);
        job5.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job5,step4_path);
        SequenceFileOutputFormat.setOutputPath(job5,step5_path);
        //TextOutputFormat.setOutputPath(job5,step5_path);
        return job5.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Step5(),args));
    }
}
