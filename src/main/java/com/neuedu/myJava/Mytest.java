package com.neuedu.myJava;

import java.io.*;

/**
 * @author Isa Huang
 */
public class Mytest {
    public static void  main(String[] args) throws IOException, ClassNotFoundException {
        //实例化对象
        Student s1 = new Student("S00000001","张三",22);
        System.out.println(s1);

        //序列化
        FileOutputStream fout = new FileOutputStream("d:/ss.dat");
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fout);
        objectOutputStream.writeObject(s1);
        objectOutputStream.close();
        fout.close();
        System.out.println("序列化完成~~");

        //反序列化
        FileInputStream fin = new FileInputStream("d:/ss.dat");
        ObjectInputStream objectInputStream = new ObjectInputStream(fin);
        Student s2 = (Student) objectInputStream.readObject();
        objectInputStream.close();
        fin.close();
        System.out.println("反序列化完成，对象信息："+s2);
    }
}
