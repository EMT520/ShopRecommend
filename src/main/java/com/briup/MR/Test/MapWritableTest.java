package com.briup.MR.Test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.HashMap;
import java.util.Map;

public class MapWritableTest {
    public static void main(String[] args) {
        MapWritable mw=new MapWritable();
        mw.put(new IntWritable(1),new Text("test"));
        mw.put(new IntWritable(2),new Text("test1"));
        mw.put(new IntWritable(3),new Text("test2"));
        mw.put(new IntWritable(4),new Text("test3"));
        mw.put(new IntWritable(5),new Text("test4"));
        //System.out.println(mw.size());
        //mw.forEach((x,y)-> System.out.println(x+"--"+y));
//        Text v=(Text)mw.get(new IntWritable(4));
//        System.out.println(v.toString());

//        System.out.println(mw.containsKey(new IntWritable(2)));
//        System.out.println(mw.containsValue(new Text("test1")));

//        for(Map.Entry<Writable,Writable> entry:mw.entrySet()){
//            IntWritable k=(IntWritable) entry.getKey();
//            Text v=(Text) entry.getValue();
//            System.out.println(k+"--"+v);
//        }

        Map<IntWritable,Text> map=new HashMap<>();
        map.put(new IntWritable(10),new Text("test10"));
        mw.putAll(map);
        mw.forEach((x,y)-> System.out.println(x+"--"+y));
    }
}
