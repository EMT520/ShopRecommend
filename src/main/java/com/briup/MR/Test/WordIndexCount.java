package com.briup.MR.Test;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

public class WordIndexCount extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

    static  class WordIndexCountMapper extends Mapper<LongWritable,Text,Text, Text> {

    public int run(String[] strings) throws Exception {
        return 0;
    }

    public static void main(String[] args) {

    }
}
}