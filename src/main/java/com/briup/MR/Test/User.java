package com.briup.MR.Test;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class User implements Writable {
    private int id;
    private String name;

    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);

    }

    public void readFields(DataInput in) throws IOException {
        this.id=in.readInt();
        this.name=in.readUTF();

    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }

    public User() {
    }

    public User(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
