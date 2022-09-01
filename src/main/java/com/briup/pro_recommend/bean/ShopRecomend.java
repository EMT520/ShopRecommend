package com.briup.pro_recommend.bean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ShopRecomend implements Writable, DBWritable {
    private long user_id;
    private long shop_id;
    private double recommend_value;

    public ShopRecomend() {
    }

    public ShopRecomend(long user_id, long shop_id, double recommend_value) {
        this.user_id = user_id;
        this.shop_id = shop_id;
        this.recommend_value = recommend_value;
    }

    public long getUser_id() {
        return user_id;
    }

    public void setUser_id(long user_id) {
        this.user_id = user_id;
    }

    public long getShop_id() {
        return shop_id;
    }

    public void setShop_id(long shop_id) {
        this.shop_id = shop_id;
    }

    public double getRecommend_value() {
        return recommend_value;
    }

    public void setRecommend_value(double recommend_value) {
        this.recommend_value = recommend_value;
    }

    @Override
    public String toString() {
        return "ShopRecomend{" +
                "user_id=" + user_id +
                ", shop_id=" + shop_id +
                ", recommend_value=" + recommend_value +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(user_id);
        dataOutput.writeLong(shop_id);
        dataOutput.writeDouble(recommend_value);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.user_id=dataInput.readLong();
        this.shop_id=dataInput.readLong();
        this.recommend_value=dataInput.readDouble();

    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setLong(1,user_id);
        preparedStatement.setLong(2,shop_id);
        preparedStatement.setDouble(3,recommend_value);

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.user_id=resultSet.getLong("user_id");
        this.shop_id=resultSet.getLong("shop_id");
        this.recommend_value=resultSet.getDouble("recommend_value");

    }
}
