package com.briup.pro_recommend.util;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ShopID implements WritableComparable<ShopID> {
    private String shopId;
    //step2 0
    //step5 1
    private int flag;
    @Override
    public int compareTo(ShopID o) {
        int n=this.shopId.compareTo(o.getShopId());
        if (n==0){
            n=this.flag-o.flag;
        }
        return n;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(shopId);
        dataOutput.writeInt(flag);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.shopId=dataInput.readUTF();
        this.flag=dataInput.readInt();

    }

    @Override
    public String toString() {
        return "ShopID{" +
                "shopId='" + shopId + '\'' +
                ", flag=" + flag +
                '}';
    }

    public ShopID(String shopId, int flag) {
        this.shopId = shopId;
        this.flag = flag;
    }

    public ShopID() {
    }

    public String getShopId() {
        return shopId;
    }

    public void setShopId(String shopId) {
        this.shopId = shopId;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }
}
