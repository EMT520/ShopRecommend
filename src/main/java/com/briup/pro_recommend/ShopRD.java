package com.briup.pro_recommend;

import com.briup.pro_recommend.bean.ShopRecomend;
import com.briup.pro_recommend.util.ShopGroup;
import com.briup.pro_recommend.util.ShopID;
import com.briup.pro_recommend.util.ShopPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/***
yarn jar first-1.0-SNAPSHOT.jar com.briup.pro_recommend.ShopRD -D input=/user/hdfs/shop

 */
public class ShopRD extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        String input = conf.get("input");
        Path step1_path = new Path("/user/hdfs/shop_result/step1");
        Path step2_path = new Path("/user/hdfs/shop_result/step2");
        Path step3_path = new Path("/user/hdfs/shop_result/step3");
        Path step4_path = new Path("/user/hdfs/shop_result/step4");
        Path step5_path = new Path("/user/hdfs/shop_result/step5");
        Path step6_path = new Path("/user/hdfs/shop_result/step6");
        Path step7_path = new Path("/user/hdfs/shop_result/step7");
        Path step8_path = new Path("/user/hdfs/shop_result/step8");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(step1_path)) {
            fs.delete(step1_path, true);
        }
        if (fs.exists(step2_path)) {
            fs.delete(step2_path, true);
        }
        if (fs.exists(step3_path)) {
            fs.delete(step3_path, true);
        }
        if (fs.exists(step4_path)) {
            fs.delete(step4_path, true);
        }
        if (fs.exists(step5_path)) {
            fs.delete(step5_path, true);
        }
        if (fs.exists(step6_path)) {
            fs.delete(step6_path, true);
        }
        if (fs.exists(step7_path)) {
            fs.delete(step7_path, true);
        }
        if (fs.exists(step8_path)) {
            fs.delete(step8_path, true);
        }

        //step1
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(Step1.class);
        job1.setJobName("step1");

        job1.setMapperClass(Step1.Step1Mapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);

        job1.setReducerClass(Step1.Step1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        TextInputFormat.addInputPath(job1, new Path(input));
        SequenceFileOutputFormat.setOutputPath(job1, step1_path);

        //step2
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(Step2.class);
        job2.setJobName("step2");

        job2.setMapperClass(Step2.Step2Mapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setReducerClass(Step2.Step2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job2, step1_path);
        SequenceFileOutputFormat.setOutputPath(job2, step2_path);

        //step3
        Job job3 = Job.getInstance(conf);
        job3.setJarByClass(Step3.class);
        job3.setJobName("step3");

        job3.setMapperClass(Step3.Step3Mapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setReducerClass(Step3.Step3Reducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job3, step1_path);
        SequenceFileOutputFormat.setOutputPath(job3, step3_path);

        //step4
        Job job4 = Job.getInstance(conf);
        job4.setJobName("step4");
        job4.setJarByClass(Step4.class);

        job4.setMapperClass(Step4.Step4Mapper.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(IntWritable.class);

        job4.setReducerClass(IntSumReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);

        job4.setInputFormatClass(SequenceFileInputFormat.class);
        job4.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job4, step3_path);
        SequenceFileOutputFormat.setOutputPath(job4, step4_path);

        //step5
        Job job5 = Job.getInstance(conf);
        job5.setJarByClass(Step5.class);
        job5.setJobName("step5");

        job5.setMapperClass(Step5.Step5Mapper.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);

        job5.setReducerClass(Step5.Step5Reducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        job5.setInputFormatClass(SequenceFileInputFormat.class);
        job5.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job5, step4_path);
        SequenceFileOutputFormat.setOutputPath(job5, step5_path);

        //step6
        Job job6 = Job.getInstance(conf);
        job6.setJobName("step6");
        job6.setJarByClass(Step6.class);

        job6.setMapperClass(Step6.Step6Mapper.class);
        job6.setMapOutputKeyClass(ShopID.class);
        job6.setMapOutputValueClass(Text.class);

        job6.setPartitionerClass(ShopPartitioner.class);
        job6.setGroupingComparatorClass(ShopGroup.class);

        job6.setReducerClass(Step6.Step6Reducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(DoubleWritable.class);

        job6.setInputFormatClass(SequenceFileInputFormat.class);
        job6.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileInputFormat.addInputPath(job6, step2_path);
        SequenceFileInputFormat.addInputPath(job6, step5_path);
        SequenceFileOutputFormat.setOutputPath(job6, step6_path);

        //step7
        Job job7 = Job.getInstance(conf);
        job7.setJarByClass(Step7.class);
        job7.setJobName("step7");

        job7.setMapperClass(Step7.Step7Mapper.class);
        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(DoubleWritable.class);

        job7.setReducerClass(Step7.Step7Reducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(DoubleWritable.class);

        job7.setInputFormatClass(SequenceFileInputFormat.class);
        job7.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job7, step6_path);
        SequenceFileOutputFormat.setOutputPath(job7, step7_path);

        //step8
        Job job8 = Job.getInstance();
        job8.setJobName("step8");
        job8.setJarByClass(Step8.class);

        //第一个map和原始文件对应
        MultipleInputs.addInputPath(job8, new Path(input), TextInputFormat.class, Step8.logMapper.class);

        //读取第七步结果
        MultipleInputs.addInputPath(job8, step7_path, SequenceFileInputFormat.class, Step8.Step8Mapper.class);
        job8.setMapOutputKeyClass(Text.class);
        job8.setMapOutputValueClass(DoubleWritable.class);

        job8.setReducerClass(Step8.Step8Reducer.class);
        job8.setOutputKeyClass(Text.class);
        job8.setOutputValueClass(DoubleWritable.class);

        job8.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job8, step8_path);

        //step9
        Job job9 = Job.getInstance(conf);
        job9.setJarByClass(Step9.class);
        job9.setJobName("step9");

        DBConfiguration.configureDB(job9.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://127.0.0.1:3306/shops",
                "root",
                "root");
        DBOutputFormat.setOutput(job9, "t_recommend_shop", "user_id", "shops_id", "recommend_value");

        job9.setMapperClass(Step9.Step9Mapper.class);
        job9.setMapOutputKeyClass(Text.class);
        job9.setMapOutputValueClass(DoubleWritable.class);

        job9.setReducerClass(Step9.Step9Reducer.class);
        job9.setOutputKeyClass(ShopRecomend.class);
        job9.setOutputValueClass(NullWritable.class);


        job9.setInputFormatClass(SequenceFileInputFormat.class);
        job9.setOutputFormatClass(DBOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job9, step8_path);

        //构建可控制的job
        ControlledJob cj1 = new ControlledJob(conf);
        cj1.setJob(job1);

        ControlledJob cj2 = new ControlledJob(conf);
        cj2.setJob(job2);
        cj2.addDependingJob(cj1);

        ControlledJob cj3 = new ControlledJob(conf);
        cj3.setJob(job3);
        cj3.addDependingJob(cj1);

        ControlledJob cj4 = new ControlledJob(conf);
        cj4.setJob(job4);
        cj4.addDependingJob(cj3);

        ControlledJob cj5 = new ControlledJob(conf);
        cj5.setJob(job5);
        cj5.addDependingJob(cj4);

        ControlledJob cj6 = new ControlledJob(conf);
        cj6.setJob(job6);
        cj6.addDependingJob(cj2);
        cj6.addDependingJob(cj5);

        ControlledJob cj7 = new ControlledJob(conf);
        cj7.setJob(job7);
        cj7.addDependingJob(cj6);

        ControlledJob cj8 = new ControlledJob(conf);
        cj8.setJob(job8);
        cj8.addDependingJob(cj7);

        ControlledJob cj9 = new ControlledJob(conf);
        cj9.setJob(job9);
        cj9.addDependingJob(cj8);

        //构建作业流
        JobControl jc = new JobControl("ShopRD");
        jc.addJob(cj1);
        jc.addJob(cj2);
        jc.addJob(cj3);
        jc.addJob(cj4);
        jc.addJob(cj5);
        jc.addJob(cj6);
        jc.addJob(cj7);
        jc.addJob(cj8);
        jc.addJob(cj9);

        Thread t = new Thread(jc);
        t.start();
        while (true) {
            for (ControlledJob c : jc.getRunningJobList()) {
                c.getJob().monitorAndPrintJob();
            }
            if (jc.allFinished()) {
                break;
            }
        }


        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ShopRD(), args));
    }
}
