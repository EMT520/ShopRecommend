package com.briup.pro_recommend;

import com.briup.pro_recommend.util.ShopGroup;
import com.briup.pro_recommend.util.ShopID;
import com.briup.pro_recommend.util.ShopPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
/*
yarn jar first-1.0-SNAPSHOT.jar com.briup.pro_recommend.Step6
hdfs dfs -cat /user/hdfs/shop_result/step6/part-r-00000
 */
public class Step6 extends Configured implements Tool {
    static class Step6Mapper extends Mapper<Text,Text, ShopID,Text>{
        private Text filename=new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fs=(FileSplit) context.getInputSplit();
            String fn=fs.getPath().getParent().getName().trim();//得到step2或step5
            //PrintWriter pw=new PrintWriter("/home/hadoop/sp.log");
            filename.set(fn);//设置文件名字
//            pw.println("----"+fn);
//            pw.flush();
//            pw.close();
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            ShopID k=new ShopID();
            k.setShopId(key.toString());//给ShopID标识
            if ("step2".equals(filename.toString())){
                k.setFlag(0);
            }else if("step5".equals(filename.toString())){
                k.setFlag(1);
            }
            context.write(k,value);
        }
    }
    static class Step6Reducer extends Reducer<ShopID,Text,Text, DoubleWritable>{
        private List<String> list=new ArrayList<>();
        @Override
        protected void reduce(ShopID key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//                Iterator<Text> iter=values.iterator();
//                String users=iter.next().toString();
//                String shops=iter.next().toString();
            for (Text val:values){
                list.add(val.toString());
            }
            if (list.size()!=2)return;
            String[] users=list.get(0).split(",");//(4:0.5 ,5:1.2, 6:0.3)
            String[] shops=list.get(1).split(",");//(20002:1,20003:1,20004:1)
            for (String user:users){
                String[] userid_hoby=user.split(":");
                double hoby_val=Double.parseDouble(userid_hoby[1]);
                for (String shop:shops){
                    String[] shopid_num=shop.split(":");
                    int num=Integer.parseInt(shopid_num[1]);
                    Double recomm_val=hoby_val*num;
                    String k=userid_hoby[0]+":"+shopid_num[0];
                    context.write(new Text(k),new DoubleWritable(recomm_val));
                }
            }
            list.clear();
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        Path step2_path=new Path("/user/hdfs/shop_result/step2");
        Path step5_path=new Path("/user/hdfs/shop_result/step5");
        Path step6_path=new Path("/user/hdfs/shop_result/step6");
        FileSystem fs=FileSystem.get(conf);
        if (fs.exists(step6_path)) {
            fs.delete(step6_path,true);
        }
        Job job6=Job.getInstance(conf);
        job6.setJobName("step6");
        job6.setJarByClass(Step6.class);

        job6.setMapperClass(Step6Mapper.class);
        job6.setMapOutputKeyClass(ShopID.class);
        job6.setMapOutputValueClass(Text.class);

        job6.setPartitionerClass(ShopPartitioner.class);
        job6.setGroupingComparatorClass(ShopGroup.class);

        job6.setReducerClass(Step6Reducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(DoubleWritable.class);

        job6.setInputFormatClass(SequenceFileInputFormat.class);

        SequenceFileInputFormat.addInputPath(job6,step2_path);
        SequenceFileInputFormat.addInputPath(job6,step5_path);

        job6.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job6,step6_path);
        //TextOutputFormat.setOutputPath(job6,step6_path);
        return job6.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Step6(),args));
    }
}
