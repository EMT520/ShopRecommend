package com.briup.pro_recommend.util;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ShopPartitioner extends Partitioner<ShopID, Text> {
    @Override
    public int getPartition(ShopID shopID, Text text, int numPartitions) {
        return Math.abs(shopID.getShopId().hashCode()) * 127 % numPartitions;
    }
}
