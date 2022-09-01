package com.briup.MR.SecondSort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondSortGroup extends WritableComparator {
    public SecondSortGroup(){
        super(YearTemp.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        YearTemp a1=(YearTemp) a;
        YearTemp b1=(YearTemp) b;

        return a1.getYear().compareTo(b1.getYear());//按年份进行分组
    }
}
