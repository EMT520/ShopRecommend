package com.briup.MR.InputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class SeqReader {
    public static void main(String[] args) throws Exception {
//// 使用hdfs的fs功能，客户端访问core-site.xml配置文件
//        Configuration conf = new Configuration();

//// 设置core-site.xml中的属性fs.defaultFS和属性值，注意主机名必须和设置的hosts主机名一致
//        conf.set("fs.defaultFS","hdfs://master:9000");

        Configuration conf = new Configuration();
        // 设置客户端访问datanode使用hostname来进行访问
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("dfs.defaultFS", "hdfs://master:9000");
        Path path = new Path("hdfs://master:9000/user/hdfs/shop_result/step1/part-r-00000");

        SequenceFile.Reader.Option op = SequenceFile.Reader.file(path);
        SequenceFile.Reader sr=new SequenceFile.Reader(conf,op);

        Writable key=(Writable) sr.getKeyClass().newInstance();
        Writable val=(Writable) sr.getValueClass().newInstance();
        while (sr.next(key,val)){
            System.out.println(key+"       "+val);
        }
    }
}