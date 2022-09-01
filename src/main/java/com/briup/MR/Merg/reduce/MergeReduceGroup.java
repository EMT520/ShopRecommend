package com.briup.MR.Merg.reduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MergeReduceGroup
        extends WritableComparator {
    public MergeReduceGroup() {
        super(TextTuple.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextTuple t = (TextTuple) a;
        TextTuple t1 = (TextTuple) b;
        return t.getUid().compareTo(t1.getUid());
    }
}