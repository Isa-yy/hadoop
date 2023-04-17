package com.neuedu.myhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 读取文本文件
 *
 * @author Isa Huang
 */
public class Demo5 {
    public static void main(String[] args) {
        try {
            //构建配置对象
            Configuration conf = new Configuration();
            //设置HADOOP集群属性,若提供core-site.xml时，无需设置，自动读取
            //conf.set("fs.defaultFS","hdfs://master:9000");
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义远程文件位置
            Path dst = new Path("/mydata/u.txt");
            //判断文件是否存在
            if(!hdfs.exists(dst)){
                System.out.println(dst.getName()+"目录不存在！！");
                return;
            }
            //创建FSDataInputStream对象
            FSDataInputStream in = hdfs.open(dst);
            //创建BufferedReadeer对象
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            //循环读取每一行文本
            String line = reader.readLine();
            while (line != null){
                //输出内容
                System.out.println(line);
                //继续读取下一行文本
                line = reader.readLine();
            }
            //关闭
            in.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}