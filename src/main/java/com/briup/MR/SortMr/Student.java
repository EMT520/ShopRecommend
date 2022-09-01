package com.briup.MR.SortMr;

public class Student implements Comparable<Student>{
    private long id;
    private String name;
    private int age;

    public Student() {
    }

    public Student(long id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
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
    //参数o是排好序的数据
    //this是即将加入排序序列的新数据
    //this.age-o.getAge()；如果得到的是负数，说明当前元素更小，则当前加入的元素放在元素o前面，反之，后面
    //如果要降序操作，return -（this.age-o.getAge()）;或者return o.getAge()-this.age;
    @Override
    public int compareTo(Student o) {
        System.out.println(this.age+":"+o.getAge()+"***"+(this.age-o.getAge()));
        return this.age-o.getAge();
    }
}
