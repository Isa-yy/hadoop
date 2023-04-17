package com.neuedu.smallfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * MapFIle写入操作
 *
 * @author Isa Huang
 */
public class SequenceFile2MapFile {
    public static void main(String[] args) {
        try {
            //构建配置对象
            Configuration conf = new Configuration();
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义SequenceFile文件所在的目录
            Path dst = new Path("/sequencefile");
            Path src = new Path(dst,MapFile.DATA_FILE_NAME);
            //读取key和value的数据类型：如果已知，可以忽略
            //定义读取器，读取key和value的数据类型
            SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(src));
            Class keyClass = reader.getKeyClass();
            Class valueClass = reader.getValueClass();
            reader.close();
            //将SequenceFile转换为MapFile文件
            long r = MapFile.fix(hdfs,dst,keyClass,valueClass,false,conf);
            System.out.println("转换成功，"+ r);
        } catch (IOException e) {
            e.printStackTrace();
        }catch (Exception e){
            throw  new RuntimeException(e);
        }
    }
}