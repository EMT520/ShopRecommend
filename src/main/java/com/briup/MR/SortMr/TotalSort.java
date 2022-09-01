package com.briup.MR.SortMr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;
/*
yarn jar first-1.0-SNAPSHOT.jar com.briup.MR.SortMr.TotalSort -D input=/user/hdfs/wea.txt -D output=/user/hdfs/wea_result1

 */
public class TotalSort
        extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String input = conf.get("input");
        String output = conf.get("output");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("totalsort");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setSortComparatorClass(MySimpleSort.class);

        job.setNumReduceTasks(2);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        TextOutputFormat.setOutputPath(job, new Path(output));
        //设置全局排序
        job.setPartitionerClass(TotalOrderPartitioner.class);
        //抽样找出各个分区的分割点，第一个参数抽取的百分比，第二个参数抽取数据的个数
        InputSampler.Sampler sample = new InputSampler.RandomSampler(0.8, 100, 10);
        InputSampler.writePartitionFile(job, sample);
        String path = TotalOrderPartitioner.getPartitionFile(conf);
        System.out.println("***:" + path);
        //将分区的结果文件分发给所有的map节点
        job.addCacheFile(new URI(path));
        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(
                new ToolRunner().run(
                        new TotalSort(), args));
    }
}
