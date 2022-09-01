package com.briup.MR.Merg.reduce;
import com.briup.MR.Merg.map.SecondStage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.Iterator;
/*
artist.txt          TextInputFormat
user_artist.txt     KeyValueTextInputFormat
yarn jar first-1.0-SNAPSHOT.jar com.briup.MR.Merg.reduce.MergeReduceMR  -D one=/user/hdfs/artist.txt -D two=/user/hdfs/user_artist.txt -D output=/user/hdfs/reduce_result

*/
public class MergeReduceMR
        extends Configured implements Tool {
    static class ArtistMapper
            extends Mapper<LongWritable, Text,TextTuple,Text>{
        StringBuffer sb=new StringBuffer();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws
                IOException, InterruptedException {
            String[] str=value.toString().split(",");
            for(int i=1;i<str.length;i++){
                sb.append(str[i]).append(",");
            }
            sb.setLength(sb.length()-1);
            TextTuple k=new TextTuple(str[0],0);
            context.write(k,new Text(sb.toString()));
            sb.setLength(0);
        }
    }
    /*
ArtistMapper->
key                     value
TextTuple(00001,0)      Moon Light,1973
TextTuple(00002,0)      Scarborough Fair,1970
*/
    static class UserArtistMapper
            extends Mapper<Text,Text,TextTuple,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException,
                InterruptedException {
            TextTuple k=
                    new TextTuple(key.toString(),1);
            context.write(k,value);
        }
    }
    /*
    UserArtistMapper->
    key                     value
    TextTuple(00001,1)      201501,50
    TextTuple(00002,1)      201501,100
    */
    static class MergeReduceMRReduce
            extends Reducer<TextTuple,Text,Text,Text>{
        @Override
        protected void reduce(TextTuple key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator iter=values.iterator();
            String artist=iter.next().toString();
            while(iter.hasNext()){
                String user_artist=iter.next().toString();
                context.write(new Text(key.getUid()),
                        new Text(artist+"#"+user_artist));
            }
        }
    }
    /*
    reduce->
    key         value
    00001       Moon Light,1973#201501,50
    */
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        String one=conf.get("one");
        String two=conf.get("two");
        String output=conf.get("output");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        Job job=Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("reduceMerge");
        //构建多个不同map
        MultipleInputs.addInputPath(job, new Path(one), TextInputFormat.class, ArtistMapper.class);
        MultipleInputs.addInputPath(job, new Path(two), KeyValueTextInputFormat.class, UserArtistMapper.class);

        job.setMapOutputKeyClass(TextTuple.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(MergeReducePartitioner.class);
        job.setGroupingComparatorClass(MergeReduceGroup.class);

        job.setNumReduceTasks(2);

        job.setReducerClass(MergeReduceMRReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job,new Path(output));
        return job.waitForCompletion(true)?0:-1;
    }
    public static void main(String[] args) throws Exception {
        System.exit(
                new ToolRunner().run(
                        new MergeReduceMR(),args));
    }
}
