package com.briup.MR.InputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SeqWriter {
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        conf.set("dfs.defaultFS","hdfs://172.26.48.119:9000");
        Path p=new Path("hdfs://172.26.48.119:9000/user/hdfs/seqwrite.txt");

        SequenceFile.Writer.Option op1=SequenceFile.Writer.file(p);
        SequenceFile.Writer.Option op2=SequenceFile.Writer.keyClass(IntWritable.class);
        SequenceFile.Writer.Option op3=SequenceFile.Writer.valueClass(Text.class);
        SequenceFile.Writer write=SequenceFile.createWriter(conf,op1,op2,op3);
        for(int i=0;i<100;i++){
            write.append(new IntWritable(i),new Text("briup"+i));
        }
        write.hflush();
        write.close();
    }
}
