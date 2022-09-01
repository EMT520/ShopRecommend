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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
/*
yarn jar first-1.0-SNAPSHOT.jar com.briup.pro_recommend.Step3
hdfs dfs -cat /user/hdfs/shop_result/step3/part-r-00000
 */
public class Step3 extends Configured implements Tool {
    static class Step3Mapper extends Mapper<Text, DoubleWritable, Text, Text> {
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            String strs[] = key.toString().split(":");
            context.write(new Text(strs[0]), new Text(strs[1]));
        }
    }

    static class Step3Reducer extends Reducer<Text, Text, Text, Text> {
        private List<String> list = new ArrayList<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                list.add(val.toString());
            }
            for (String s : list) {
                for (String s1 : list) {
                    context.write(new Text(s), new Text(s1));
                }
            }
            list.clear();
        }

    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Path step1_path = new Path("/user/hdfs/shop_result/step1");
        Path step3_path = new Path("/user/hdfs/shop_result/step3");
        FileSystem fs=FileSystem.get(conf);

        if (fs.exists(step3_path)) {
            fs.delete(step3_path,true);
        }
        Job job3 = Job.getInstance(conf);
        job3.setJarByClass(Step3.class);
        job3.setJobName("step3");

        job3.setMapperClass(Step3Mapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setReducerClass(Step3Reducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job3,step1_path);
        SequenceFileOutputFormat.setOutputPath(job3,step3_path);
        //TextOutputFormat.setOutputPath(job3,step3_path);

        return job3.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Step3(), args));
    }
}
