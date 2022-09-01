package com.briup.MR.Test;

import org.apache.hadoop.io.ArrayPrimitiveWritable;

import java.util.Arrays;

public class ArrayPrimitiveWritableTest {
    public static void main(String[] args) {
        int[] n={1,2,3,4,5};
//        System.out.println(n.getClass());
//        ArrayPrimitiveWritable aw=new ArrayPrimitiveWritable();

//        ArrayPrimitiveWritable aw=new ArrayPrimitiveWritable(int.class);
//        aw.set(n);

        ArrayPrimitiveWritable aw=new ArrayPrimitiveWritable(n);

        int[] res=(int[]) aw.get();
        System.out.println(Arrays.toString(res));

    }
}
