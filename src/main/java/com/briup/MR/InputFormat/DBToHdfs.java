package com.briup.MR.InputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
/*
yarn jar first-1.0-SNAPSHOT.jar com.briup.MR.InputFormat.DBToHdfs -D output=/user/hdfs/db_hdfs_resul
 */
public class DBToHdfs extends Configured implements Tool {
        static class DBToHdfsMapper
            extends Mapper<LongWritable,Teacher,LongWritable,Teacher>{
            @Override
            protected void map(LongWritable key, Teacher value, Context context) throws IOException, InterruptedException {
                context.write(key,value);
            }
        }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        String output=conf.get("output");

        Job job=Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("DbToHdfs");
        //设置连接数据库的四要素
        DBConfiguration.configureDB(job.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://127.0.0.1:3306/mr",
                "root",
                "root");
        DBInputFormat.setInput(job,Teacher.class,"teacher",
                "1=1","id","id","name","age");
        job.setMapperClass(DBToHdfsMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Teacher.class);

        job.setNumReduceTasks(0);


        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(output));
        return job.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception{
        System.exit(new ToolRunner().run(new DBToHdfs(),args));
    }
}
