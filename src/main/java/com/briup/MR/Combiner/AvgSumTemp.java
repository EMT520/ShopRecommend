package com.briup.MR.Combiner;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgSumTemp implements Writable {
    private int num;
    private Double sum_temp;

    public AvgSumTemp() {
    }

    public AvgSumTemp(int num, Double sum_temp) {
        this.num = num;
        this.sum_temp = sum_temp;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public Double getSum_temp() {
        return sum_temp;
    }

    public void setSum_temp(Double sum_temp) {
        this.sum_temp = sum_temp;
    }

    @Override
    public String toString() {
        return "AvgSumTemp{" +
                "num=" + num +
                ", sum_temp=" + sum_temp +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(num);
        dataOutput.writeDouble(sum_temp);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.num=dataInput.readInt();
        this.sum_temp=dataInput.readDouble();

    }
}
