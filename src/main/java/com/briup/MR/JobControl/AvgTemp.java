package com.briup.MR.JobControl;

import com.briup.MR.Combiner.AvgSumTemp;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgTemp {
    static class AvgTempMapper extends Mapper<Text, AvgSumTemp, Text, DoubleWritable>{
        @Override
        protected void map(Text key, AvgSumTemp value, Context context) throws IOException, InterruptedException {
            context.write(key,new DoubleWritable(value.getSum_temp()/value.getNum()));
        }
    }
}
