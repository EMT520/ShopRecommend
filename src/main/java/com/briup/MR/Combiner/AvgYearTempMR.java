package com.briup.MR.Combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
/*
yarn jar first-1.0-SNAPSHOT.jar com.briup.MR.Combiner.AvgYearTempMR -D input=/user/hdfs/wea.txt -D output=/user/hdfs/avg_result
hdfs dfs -cat /user/hdfs/avg_result/part-r-00000
 */
public class AvgYearTempMR extends Configured implements Tool {
    static class AvgYearTempMRMapper extends Mapper<LongWritable, Text,Text,AvgSumTemp>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] val=value.toString().split(",");
            AvgSumTemp as=new AvgSumTemp(1,Double.parseDouble(val[1]));
            context.write(new Text(val[0]),as);
        }
    }
    static class AvgYearTempMRCombiner extends Reducer<Text,AvgSumTemp,Text,AvgSumTemp>{
        @Override
        protected void reduce(Text key, Iterable<AvgSumTemp> values, Context context) throws IOException, InterruptedException {
            int num=0;
            double sum_temp=0;
            for (AvgSumTemp as:values){
                num+=as.getNum();
                sum_temp+=as.getSum_temp();
            }
            context.write(key,new AvgSumTemp(num,sum_temp));
        }
    }
    static class AvgYearTempMRReduce extends Reducer<Text,AvgSumTemp,Text, DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<AvgSumTemp> values, Context context) throws IOException, InterruptedException {
            int num=0;
            double sum_temp=0;
            for (AvgSumTemp as:values){
                num+=as.getNum();
                sum_temp+=as.getSum_temp();
            }
            context.write(key,new DoubleWritable(sum_temp/num));
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        String input=conf.get("input");
        String output=conf.get("output");

        Job job=Job.getInstance(conf);
        job.setJarByClass(AvgYearTempMR.class);
        job.setJobName("Average Temperature By Year");

        job.setMapperClass(AvgYearTempMRMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvgSumTemp.class);

        job.setCombinerClass(AvgYearTempMRCombiner.class);

        job.setReducerClass(AvgYearTempMRReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job,new Path(input));
        TextOutputFormat.setOutputPath(job,new Path(output));

        return job.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new AvgYearTempMR(),args));
    }
}
