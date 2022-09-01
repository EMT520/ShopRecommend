import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;

public class mkdir {
    @Test
    public void mkdir() {
        try {
            //创建Configuration对象
            Configuration conf = new Configuration();
            //设置Hadoop的dfs客户端使用hostname访问datanode
            conf.set("dfs.client.use.datanode.hostname", "true");


            conf.set("fs.defaultFS", "hdfs://master:9000");
            FileSystem fs = FileSystem.get(conf);
            boolean result = fs.mkdirs(new Path("/user/hdfs/test"));
            System.out.println("创建文件夹结果：{}"+result);
        } catch (Exception e) {
            System.out.println("创建文件夹出错:"+ e);
        }
    }
    /**
     * 写入文件
     */
    @Test
    public  void write() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        //设置Hadoop的dfs客户端使用hostname访问datanode
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fs = FileSystem.get(conf);
        byte[] buff = "Hello hadoop".getBytes(); // 要写入的内容
        String filename = "hdfs://master:9000//user/hdfs/test/a"; //要写入的文件名
        FSDataOutputStream os = fs.create(new Path(filename));
        os.write(buff,0,buff.length);
        System.out.println("写入文件："+new Path(filename).getName());
        os.close();
        fs.close();
    }
    /**
     * 判断文件是否存在
     */
    @Test
    public void isExistFile() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);

        String filename = "hdfs://master:9000/user/hdfs/test";//文件路径
        if(fs.exists(new Path(filename))){
            System.out.println("文件存在");
        }else{
            System.out.println("文件不存在");
        }
        fs.close();
    }
    @Test
    /*
    读取文件
     */
    public void read() throws IOException{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);

        // 打开hdfs上的文件并读取输出
        Path hello = new Path("/user/hdfs/shop_result/step1/part-r-00000");
        FSDataInputStream ins = fs.open(hello);
        int ch = ins.read();
        while (ch != -1) {
            System.out.print((char)ch);
            ch = ins.read();
        }
        System.out.println();
        fs.close();

    }
    /**
     * 遍历文件夹
     */
    @Test
    public void listFiles() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);


        FileStatus[] statuses = fs.listStatus(new Path("hdfs://master:9000/user/hdfs/shop_result/step1/"));
        for (FileStatus file : statuses) {
            if(file.isFile()){
                //是文件
                System.out.println("扫描到文件:"+file.getPath().getName());
            }else{
                //不是文件
                System.out.println("扫描到文件夹:"+file.getPath().getName());
            }

        }
        fs.close(); //关闭hdfs
    }
    /**
     * 删除文件
     */
    @Test
    public void delete() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://116.62.33.159:9000");
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path("hdfs://116.62.33.159:9000/user/hdfs/test/");
        boolean result = fs.delete(file, true);
        System.out.println("删除文件结果:"+result);
        fs.close(); //关闭hdfs
    }
    @Test
    public void runHdfsLearn() throws IOException{
        mkdir();//创建文件夹
        isExistFile();//判断文件是否存在
        write();//写入文件
        isExistFile();//在判断文件是否存在
        read();//读取文件内容
        delete();//删除文件
        isExistFile();//在判断文件是否存在
        listFiles();//遍历主目录
    }
}
