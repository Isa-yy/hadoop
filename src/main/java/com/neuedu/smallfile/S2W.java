package com.neuedu.smallfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * 逐字节读取文件
 *
 * @author Isa Huang
 */
public class S2W {
    public static void main(String[] args) {
        try {
            //构建配置对象
            Configuration conf = new Configuration();
            //设置HADOOP集群属性,若提供core-site.xml时，无需设置，自动读取
            //conf.set("fs.defaultFS","hdfs://master:9000");
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义目标路径
            Path dst = new Path("/sequencefile/s.dat");
            //定义读取器
            SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(dst));
            //定义文件名变量、文件内容变量
            IntWritable file = new IntWritable();
            Text concent = new Text();
            //循环读取
            while (reader.next(file,concent)){
                System.out.println("文件名："+ file);
                System.out.println("文件内容"+concent);
            }
            //关闭读取器
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}