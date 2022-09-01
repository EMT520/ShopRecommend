package com.briup.MR.SortMr;

import com.briup.MR.partitionMR.PhonePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
yarn jar first-1.0-SNAPSHOT.jar com.briup.MR.SortMr.SimpleSortMR -D input=/user/hdfs/wea.txt -D output=/user/hdfs/wea_result
局部排序
 */
public class SimpleSortMR extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        String input=conf.get("input");
        String output=conf.get("output");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");

        Job job2=Job.getInstance(conf);
        job2.setJarByClass(this.getClass());
        job2.setJobName("SimpleSortMR");

        job2.setMapperClass(Mapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setSortComparatorClass(MySimpleSort.class);
        //job2.setPartitionerClass(PhonePartitioner.class);
        job2.setNumReduceTasks(2);

//        job2.setReducerClass(Reducer.class);
//        job2.setMapOutputKeyClass(Text.class);
//        job2.setMapOutputValueClass(Text.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job2,new Path(input));
        TextOutputFormat.setOutputPath(job2,new Path(output));
        return job2.waitForCompletion(true)?0:-1;


    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new SimpleSortMR(),args));
    }
}