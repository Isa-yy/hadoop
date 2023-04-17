package com.neuedu.smallfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * MapFIle读取操作
 *
 * @author Isa Huang
 */
public class M2R {
    public static void main(String[] args) {
        try {
            //构建配置对象
            Configuration conf = new Configuration();
            //定义远程文件位置
            Path dst = new Path("/mapfile/m");
            //定义读取器
            MapFile.Reader reader = new MapFile.Reader(dst,conf);
            //定义文件名和内容的变量
            IntWritable fileName = new IntWritable();
            Text content = new Text();
            //循环读取所有的小文件
            while (reader.next(fileName, content)){
                //输入文件信息
                System.out.println("文件名："+fileName);
                System.out.println("文件内容："+content);
            }
            //关闭
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}