package com.briup.MR.JobControl;

import com.briup.MR.Combiner.AvgSumTemp;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SumTemp {
    static class SumTempMapper extends Mapper<LongWritable, Text,Text, AvgSumTemp>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] val=value.toString().split(",");
            AvgSumTemp as=new AvgSumTemp(1,Double.parseDouble(val[1]));
            context.write(new Text(val[0]),as);
        }
    }
    static class SumTempReduce extends Reducer<Text,AvgSumTemp,Text,AvgSumTemp>{
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
}
