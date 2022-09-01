package com.briup.MR.SecondSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
/*
key                 value
YearTemp(1992,23)   23
YearTemp(1992,34)   34
YearTemp(1993,22)   22
YearTemp(1993,25)   25
YearTemp(1994,22)   22
YearTemp(1995,27)   27
分区
0号分区(年份为偶数)
YearTemp(1992,23)   23
YearTemp(1992,34)   34
YearTemp(1994,22)   22
1号分区
YearTemp(1993,22)   22
YearTemp(1993,25)   25
YearTemp(1995,27)   27
map端结束

reduce端开始

reduce0
YearTemp(1992,23)   23
YearTemp(1992,34)   34
YearTemp(1994,22)   22
->分组：年份相同的为一组，按照温度升序
0组
YearTemp(1992,23)   23
YearTemp(1992,34)   34
1组
YearTemp(1994,22)   22

->合并
YearTemp(1992,34)   [23，34]
YearTemp(1994,22)   [22]

reduce1
0组
YearTemp(1993,22)   22
YearTemp(1993,25)   25
1组
YearTemp(1995,27)   27

->合并:
YearTemp(1993,25)   [22,25]
YearTemp(1995,27)   [27]

yarn jar first-1.0-SNAPSHOT.jar com.briup.MR.SecondSort.SecondSortMr -D input=/user/hdfs/wea.txt -D output=/user/hdfs/sort_result
 */
public class SecondSortMr extends Configured implements Tool {
    static class SecondSortMrMapper extends Mapper<Text,Text,YearTemp, DoubleWritable>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String year=key.toString();
            double temp=Double.parseDouble(value.toString());
            YearTemp k=new YearTemp(year,temp);
            context.write(k,new DoubleWritable(temp));
        }
    }
    static class SecondSortMrReducer extends Reducer<YearTemp,DoubleWritable,YearTemp,Text>{
        private StringBuffer sb=new StringBuffer();
        @Override
        protected void reduce(YearTemp key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable val:values){
                sb.append(val.get()).append("*");
            }
            sb.setLength(sb.length()-1);
            context.write(key,new Text(sb.toString()));
            sb.setLength(0);

            //for
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        String input=conf.get("input");
        String output=conf.get("output");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
//        FileSystem fs=FileSystem.get(conf);
//        if (fs.exists(new Path(output))) {
//            fs.delete(Path(output),true);
//        }

        Job job=Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("secondsort");


        job.setMapperClass(SecondSortMrMapper.class);
        job.setMapOutputKeyClass(YearTemp.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setPartitionerClass(SecondSortPartitioner.class);
        job.setGroupingComparatorClass(SecondSortGroup.class);
        job.setNumReduceTasks(2);

        job.setReducerClass(SecondSortMrReducer.class);
        job.setOutputKeyClass(YearTemp.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        KeyValueTextInputFormat.addInputPath(job,new Path(input));
        TextOutputFormat.setOutputPath(job,new Path(output));
        return job.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new SecondSortMr(),args));
    }
}
