package com.briup.MR.Merg.reduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextTuple implements WritableComparable<TextTuple> {
    private String uid;
    //flag ->0 表示artis.txt 为1表示user_artis.txt
    private int flag;

    @Override
    public String toString() {
        return "TextTuple{" +
                "uid='" + uid + '\'' +
                ", flag=" + flag +
                '}';
    }

    public TextTuple() {
    }

    public TextTuple(String uid, int flag) {
        this.uid = uid;
        this.flag = flag;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(uid);
        out.writeInt(flag);
    }

    public void readFields(DataInput in) throws IOException {
        this.uid = in.readUTF();
        this.flag = in.readInt();
    }

    public int compareTo(TextTuple o) {
        //this 00001    o 00002
        int n = this.getUid().compareTo(o.getUid());
        if (n != 0) {
            return n;
        }
        return this.getFlag() - o.getFlag();
    }
}