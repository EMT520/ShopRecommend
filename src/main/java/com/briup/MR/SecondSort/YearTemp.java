package com.briup.MR.SecondSort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearTemp implements WritableComparable<YearTemp> {
    private String year;
    private Double temp;

    public YearTemp() {
    }

    public YearTemp(String year, Double temp) {
        this.year = year;
        this.temp = temp;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public Double getTemp() {
        return temp;
    }

    public void setTemp(Double temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        return "YearTemp{" +
                "year='" + year + '\'' +
                ", temp=" + temp +
                '}';
    }

    //参数是排好序的数据
    //当前对象是将要加入的对象
    @Override
    public int compareTo(YearTemp o) {
        int n=-(this.year.compareTo(o.getYear()));
        if (n==0){
            //n=(int)(this.temp-o.getTemp());
            //this.getTemp().compareTo(o.getTemp());
            n=(this.temp<o.getTemp())?1:-1;
        }
        return n;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeDouble(temp);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year=dataInput.readUTF();
        this.temp=dataInput.readDouble();

    }

}
