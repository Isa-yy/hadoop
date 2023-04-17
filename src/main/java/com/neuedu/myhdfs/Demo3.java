package com.neuedu.myhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * 逐字节读取文件
 *
 * @author Isa Huang
 */
public class Demo3 {
    public static void main(String[] args) {
        try {
            //构建配置对象
            Configuration conf = new Configuration();
            //设置HADOOP集群属性,若提供core-site.xml时，无需设置，自动读取
            //conf.set("fs.defaultFS","hdfs://master:9000");
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义远程文件位置
            Path dst = new Path("/mydata/china.txt");
            //判断文件是否存在
            if(!hdfs.exists(dst)){
                System.out.println(dst.getName()+"目录不存在！！");
                return;
            }
            //创建FSDataInputStream对象
            FSDataInputStream in = hdfs.open(dst);
            //循环逐字节读取类容
            int b = in.read();
            while (b != -1){
                //输出内容,将字节转换成字符，再输出
                System.out.println((char)b);
                //继续读下一个字符
                b = in.read();
            }
            //关闭
            in.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}