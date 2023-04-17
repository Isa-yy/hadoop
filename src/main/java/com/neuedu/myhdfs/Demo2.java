package com.neuedu.myhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * 新建文件，内容写入
 * @author Isa Huang
 */
public class Demo2 {
    public static void main(String[]args){
        try {
            //构建配置对象
            Configuration conf = new Configuration();
            //设置HADOOP集群属性,若提供core-site.xml时，无需设置，自动读取
            //conf.set("fs.defaultFS","hdfs://master:9000");
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义写入文件的内容
            String[] str = {"A B C","KK LL OO","ppojjhba jjzmm"};
            //定义远程文件位置
            Path dst = new Path("/mydata/china.txt");
            //创建对象
            FSDataOutputStream out = hdfs.create(dst,true);
            //循环写入数据
            for(String line:str){
                out.writeUTF(line);
                out.flush();
            }
            //关闭
            out.close();
            //提示信息
            if(hdfs.exists(dst)){
                System.out.println("文件新建并写入成功~~~");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}