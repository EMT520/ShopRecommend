package com.briup.pro_recommend;

import com.briup.MR.InputFormat.HdfsToDb;
import com.briup.MR.InputFormat.Teacher;
import com.briup.pro_recommend.bean.ShopRecomend;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

/*
step8->
SequnceFileInputFormat
Text        DoubleWritable
4:20001     3.2
->map{

}
key         value
4:20001     3.2
->reduce{
}
key             value
ShopRecommend   null
 */
/*
yarn jar first-1.0-SNAPSHOT.jar com.briup.pro_recommend.Step9

 */
public class Step9 extends Configured implements Tool {
    static class Step9Mapper extends Mapper<Text, DoubleWritable,Text,DoubleWritable> {
        @Override
        protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }
    static class Step9Reducer extends Reducer<Text,DoubleWritable, ShopRecomend, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            String[] userid_shopid=key.toString().split(":");
            for (DoubleWritable val:values){
                ShopRecomend sr=new ShopRecomend(Long.parseLong(userid_shopid[0]),Long.parseLong(userid_shopid[1]),val.get());
                context.write(sr,NullWritable.get());
            }
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        Path step8_path=new Path("/user/hdfs/shop_result/step8");

        //job作业执行前把表删除，写jdbc程序

        

        Job job9=Job.getInstance(conf);
        job9.setJarByClass(this.getClass());
        job9.setJobName("step9");

        DBConfiguration.configureDB(job9.getConfiguration(),
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://127.0.0.1:3306/shops",
                "root",
                "root");
        DBOutputFormat.setOutput(job9,"t_recommend_shop", "user_id","shops_id","recommend_value");

        job9.setMapperClass(Step9Mapper.class);
        job9.setMapOutputKeyClass(Text.class);
        job9.setMapOutputValueClass(DoubleWritable.class);

        job9.setReducerClass(Step9Reducer.class);
        job9.setOutputKeyClass(ShopRecomend.class);
        job9.setOutputValueClass(NullWritable.class);


        job9.setInputFormatClass(SequenceFileInputFormat.class);
        job9.setOutputFormatClass(DBOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job9,step8_path);
        return job9.waitForCompletion(true)?0:-1;

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Step9(),args));
    }
}
