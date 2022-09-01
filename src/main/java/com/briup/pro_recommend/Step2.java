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

/***
yarn jar first-1.0-SNAPSHOT.jar com.briup.pro_recommend.Step2
hdfs dfs -cat /user/hdfs/shop_result/step2/part-r-00000
 */
public class Step2 extends Configured implements Tool {


    static class Step2Mapper extends Mapper<Text, DoubleWritable,Text,Text> {
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            String[] ks=key.toString().split(":");
                context.write(new Text(ks[1]),new Text(ks[0]+":"+value.get()));
            }
        }

    static class Step2Reducer extends Reducer<Text,Text,Text,Text> {
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

            Path step1_path=new Path("/user/hdfs/shop_result/step1");
            Path step2_path=new Path("/user/hdfs/shop_result/step2");
            FileSystem fs=FileSystem.get(conf);
            if (fs.exists(step2_path)) {
                fs.delete(step2_path,true);
            }
            Job job2=Job.getInstance(conf);
            job2.setJarByClass(Step2.class);
            job2.setJobName("step2");

            job2.setMapperClass(Step2Mapper.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setReducerClass(Step2Reducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileInputFormat.addInputPath(job2,step1_path);
            SequenceFileOutputFormat.setOutputPath(job2,step2_path);
            //TextOutputFormat.setOutputPath(job2,step2_path);
            return job2.waitForCompletion(true)?0:-1;
        }


    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Step2(),args));
    }
}
