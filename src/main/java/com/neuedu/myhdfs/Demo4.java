package com.neuedu.myhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * 按块读取文件
 *
 * @author Isa Huang
 */
public class Demo4 {
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
            //定义块
            byte[] block = new byte[16];
            //获取文件总大小
            int size = in.available();
            //计算块的总数
            int blockNumber = size / block.length;
            //计算剩余字节数
            int lastSize = size % block.length;
            //读取1：读取N个块
            for (int i = 0;i<blockNumber;i++){
                //读取块内容，并输出
                int b = in.read(block);
                //将字节数组转换为字符串
                String blockData = new String(block);
                System.out.print(blockData);
            }
            //读取2：剩余字节内容
            if(lastSize > 0) {
                //读取最后字节
                byte[] last = new byte[lastSize];
                int b = in.read(last);
                //将字节数组转换为字符串
                String lastData  = new String(last);
                System.out.print(lastData);
            }
            //关闭
            in.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}