package com.briup.MR.SortMr;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
public class MySimpleSort extends WritableComparator {
    public MySimpleSort(){
        super(Text.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Text y= (Text) a;
        Text y1= (Text) b;
        return -y.compareTo(y1);
    }
}