package com.briup.MR.partitionMR;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
public class PhonePartitioner extends Partitioner<Text,Text> {
    @Override
    public int getPartition(Text text, Text text2, int numPartitions) {
        String phone = text.toString();
        if (phone.startsWith("138")) {
            return 0;
        } else if (phone.startsWith("188")) {
            return 1;
        }
        return 2;
    }
}