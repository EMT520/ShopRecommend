package com.briup.MR.Merg.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;
/*
yarn jar first-1.0-SNAPSHOT.jar com.briup.MR.Merg.map.SecondStage  -D one=/user/hdfs/artist.txt -D two=/user/hdfs/user_artist.txt -D output=/user/hdfs/m_result

 */
public class SecondStage extends Configured implements Tool {
    static  class SecondStageMapper extends Mapper<Text, TupleWritable,Text,Text>{
        private  StringBuffer sb=new StringBuffer();

        @Override
        protected void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException {
            for (Writable val:value){
                sb.append(val.toString()).append("#");
            }
            sb.setLength(sb.length()-1);
            context.write(key,new Text(sb.toString()));
            sb.setLength(0);
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        String one=conf.get("one");
        String two=conf.get("two");
        String output=conf.get("output");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");//设置分隔符

        String exp= CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class,new Path(one),new Path(two));
        conf.set("mapreduce.join.expr",exp);

        Job job=Job.getInstance(conf);
        job.setJobName("secondstage");
        job.setJarByClass(this.getClass());

        job.setMapperClass(SecondStageMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(CompositeInputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(output));
        return job.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(new ToolRunner().run(new SecondStage(),args));
    }
}
