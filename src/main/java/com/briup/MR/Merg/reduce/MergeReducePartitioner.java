package com.briup.MR.Merg.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MergeReducePartitioner
        extends Partitioner<TextTuple, Text> {
    public int getPartition(TextTuple textTuple, Text text, int numPartitions) {
        return Math.abs(textTuple.getUid().hashCode()) * 127 % numPartitions;
    }
}
