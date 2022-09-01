package com.briup.MR.Merg.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/***
1.将连接的数据排序
2.文件不可拆分，生成Gzip格式
3.reduce个数一致
->TextInputFormat
reduce不处理

yarn jar first-1.0-SNAPSHOT.jar com.briup.MR.Merg.map.FirstStage -D input=/user/hdfs/artist.txt -D output=/user/hdfs/one
yarn jar first-1.0-SNAPSHOT.jar com.briup.MR.Merg.map.FirstStage -D input=/user/hdfs/user_artist.txt -D output=/user/hdfs/two

 */
public class FirstStage extends Configured implements Tool {
    static class FirstStageMapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strs[]=value.toString().split(",");
            context.write(new Text(strs[0]),value);
        }
    }
    static class FirstStageReduce extends Reducer<Text,Text, NullWritable,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val:values){
                context.write(NullWritable.get(),val);
            }
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        String input=conf.get("input");
        String output=conf.get("output");

        Job job=Job.getInstance(conf);
        job.setJobName("firststage");
        job.setJarByClass(this.getClass());

        job.setMapperClass(FirstStageMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(FirstStageReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //默认的，可以不写这两行
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job,new Path(input));
        TextOutputFormat.setOutputPath(job,new Path(output));
        //设置压缩格式
        TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        return job.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new FirstStage(),args));
    }
}
