package com.neuedu.myhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * 上传文件
 *
 * @author Isa Huang
 */
public class Demo1 {
 public static void main(String[]args){
     try {
         //构建配置对象
         Configuration conf = new Configuration();
         //设置HADOOP集群属性,若提供core-site.xml时，无需设置，自动读取
         //conf.set("fs.defaultFS","hdfs://master:9000");
         //获取HDFS对象
         FileSystem hdfs = FileSystem.get(conf);
         //定义本地文件位置
         Path src = new Path("d:/u.txt");
         //定义远程文件位置
         Path dst = new Path("/mydata/y.txt");
         //上传文件
         hdfs.copyFromLocalFile(src,dst);
         //提示信息
         System.out.println("文件上传成功~~~");
     } catch (IOException e) {
         throw new RuntimeException(e);
     }

 }
}
