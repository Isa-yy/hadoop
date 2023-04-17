package com.neuedu.myhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * 其他操作
 *
 * @author Isa Huang
 */
public class Demo7 {
    public static void main(String[] args) {
        try {
            //构建配置对象
            Configuration conf = new Configuration();
            //设置HADOOP集群属性,若提供core-site.xml时，无需设置，自动读取
            //conf.set("fs.defaultFS","hdfs://master:9000");
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义远程文件位置
            Path dst = new Path("/f.txt");
            //判断文件是否存在
            if(!hdfs.exists(dst)){
                System.out.println(dst.getName()+"文件不存在！！");
            }
            //删除文件
            boolean flage = hdfs.delete(dst,true);
            if (flage){
                System.out.println(dst.getName()+"文件已删除");
            }
            //定义目标路径
            Path src = new Path("/books/f.txt");
            //查看块的信息
            FileStatus fs = hdfs.getFileLinkStatus(src);
            BlockLocation[] bloocks = hdfs.getFileBlockLocations(fs,0,fs.getLen());
            for(BlockLocation b:bloocks){
                for (int i=0;i<b.getNames().length;i++){
                    System.out.println(b.getNames()[i]);
                    System.out.println(b.getOffset());
                    System.out.println(b.getLength());
                    System.out.println(b.getHosts()[i]);
                }
                System.out.println("-------------------------------");
            }
            //循环每个块
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}