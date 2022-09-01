package com.briup.MR.JobControl;

import com.briup.MR.Combiner.AvgSumTemp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
yarn jar first-1.0-SNAPSHOT.jar com.briup.MR.JobControl.AvgWorkFlow -D input=/user/hdfs/wea.txt -D temp_input=/user/hdfs/temp_input -D output=/user/hdfs/avg_result1
hdfs dfs -cat /user/hdfs/avg_result1/part-m-00000

 */
public class AvgWorkFlow extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        String input=conf.get("input");
        String temp_input=conf.get("temp_input");
        String output=conf.get("output");
        Job job1=Job.getInstance(conf);
        job1.setJarByClass(this.getClass());
        job1.setJobName("sum_temp");

        job1.setMapperClass(SumTemp.SumTempMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(AvgSumTemp.class);

        job1.setReducerClass(SumTemp.SumTempReduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(AvgSumTemp.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        TextInputFormat.addInputPath(job1,new Path(input));
        SequenceFileOutputFormat.setOutputPath(job1,new Path(temp_input));

        Job job2=Job.getInstance(conf);
        job2.setJarByClass(this.getClass());
        job2.setJobName("avg_temp");

        job2.setMapperClass(AvgTemp.AvgTempMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DoubleWritable.class);

        job2.setNumReduceTasks(0);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job2,new Path(temp_input));
        TextOutputFormat.setOutputPath(job2,new Path(output));
        //构建作业流
        ControlledJob cron_tab1=new ControlledJob(conf);
        cron_tab1.setJob(job1);
        ControlledJob cron_tab2=new ControlledJob(conf);
        cron_tab2.setJob(job2);
        //添加依赖,作业2依赖作业1，先执行作业1，再执行作业2
        cron_tab2.addDependingJob(cron_tab1);

        //构建作业组
        JobControl jobs=new JobControl("work_flow");
        jobs.addJob(cron_tab1);
        jobs.addJob(cron_tab2);

        //作业运行基于线程
        Thread t=new Thread(jobs);
        t.start();
        //监控作业
        while (true){
            for (ControlledJob c:jobs.getRunningJobList()){
                c.getJob().monitorAndPrintJob();
            }
            if (jobs.allFinished()) {
                break;
            }
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new AvgWorkFlow(),args));
    }
}
