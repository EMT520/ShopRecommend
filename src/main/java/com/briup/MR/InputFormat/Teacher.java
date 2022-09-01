package com.briup.MR.InputFormat;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

//mr读取数据库表，DBinputformat读取数据库表的一行，转化为一组键值对
//键位行号，值为封装对象
public class Teacher implements WritableComparable<Teacher>, DBWritable {
    private long id;
    private String name;
    private int age;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeUTF(name);
        out.writeInt(age);

    }
    //序列化和反序列化的顺序必须一致
    @Override
    public void readFields(DataInput in) throws IOException {
        this.id=in.readLong();
        this.name=in.readUTF();
        this.age=in.readInt();
    }
    //sql=insert into teacher(id,name,age) values(?,?,?)
    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setLong(1,this.id);
        preparedStatement.setString(2,this.name);
        preparedStatement.setInt(3,age);

    }
    //insert id,name,age from teacher;
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id=resultSet.getLong("id");
        this.name=resultSet.getString("name");
        this.age=resultSet.getInt("age");
    }

    public Teacher() {
    }

    public Teacher(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public Teacher(long id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "Teacher{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }



    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public int compareTo(Teacher o) {
        return this.getAge()-o.getAge();
    }
}
