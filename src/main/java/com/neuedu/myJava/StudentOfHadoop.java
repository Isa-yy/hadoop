package com.neuedu.myJava;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实体类Student,需要实现序列化接口
 * @author Isa Huang
 */
public class StudentOfHadoop implements WritableComparable<StudentOfHadoop> {
    private String id;
    private String name;
    private Integer age;

    public StudentOfHadoop(){

    }

    public StudentOfHadoop(String id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
    public int compareTo(StudentOfHadoop other) {
        if(null==other){
            return 1;
        }
        //默认比较学号
        return this.id.compareTo(other.id);
    }

    public void write(DataOutput out) throws IOException {
        //hadoop 序列化
        out.writeUTF(this.id);
        out.writeUTF(this.name);
        out.writeInt(this.age);

    }

    public void readFields(DataInput in) throws IOException {
        //反序列化(注意顺序)
        this.id = in.readUTF();
        this.name = in.readUTF();
        this.age = in.readInt();

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }



}
