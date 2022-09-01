package com.briup.MR.Test;

import org.apache.hadoop.io.ArrayWritable;

public class ArrayWritableTest {
    public static void main(String[] args) {
//        String[] strs={"lisi","tom","jake"};
//        ArrayWritable aw=new ArrayWritable(strs);
//        Writable[] n=aw.get();
        //1.
        //方法一
//        for(Writable n1:n){
//        System.out.println(n1.toString());
//    }
        //方法二
//        String [] s=aw.toStrings();
//        System.out.println(s[0]);

        //2.
//        IntWritable[] arr={new IntWritable(1),new IntWritable(2)};
//        ArrayWritable a=new ArrayWritable(IntWritable.class);
//        a.set(arr);
//        //System.out.println(Arrays.toString(a.toStrings()));//Array.toString转换为数组
//        for(Writable n1:a.get()){
//            IntWritable n2=(IntWritable) n1;
//            System.out.println(n2.get());
//        }

        //3.
//        IntWritable[] arr={new IntWritable(1),new IntWritable(2)};
//        ArrayWritable a=new ArrayWritable(IntWritable.class,arr);
//        for(Writable n1:a.get()){
//            IntWritable n2=(IntWritable) n1;
//            System.out.println(n2.get());
//        }




        User[] users={new User(1,"jake"),
            new User(2,"tom")};
        ArrayWritable aw=new ArrayWritable(User.class,users);
        User[] us=(User[]) aw.get();
        for(User u:us){
            System.out.println(u);
        }

    }
}