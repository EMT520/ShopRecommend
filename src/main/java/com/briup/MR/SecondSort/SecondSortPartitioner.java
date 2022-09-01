package com.briup.MR.SecondSort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondSortPartitioner extends Partitioner<YearTemp, DoubleWritable> {
    @Override
    public int getPartition(YearTemp yearTemp, DoubleWritable doubleWritable, int numPartition) {
        return Integer.parseInt(yearTemp.getYear())%numPartition;
    }
}
