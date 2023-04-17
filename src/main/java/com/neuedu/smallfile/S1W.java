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
 * SequenceFile写操作
 *
 * @author Isa Huang
 */
public class S1W {
    public static void main(String[] args) {
        //定义文件内容
        String[] data = {"hhhhhhh uuuuuuu",
                "llllll uuuuuu",
                "qqqqqqqq iiiiiii",
                "oooooooooo wwwww"};
        try {
            //构建配置对象
            Configuration conf = new Configuration();
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义目标路径
            Path dst = new Path("/sequencefile/s.dat");
            SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(dst),
                    SequenceFile.Writer.keyClass(IntWritable.class),
                    SequenceFile.Writer.valueClass(Text.class),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
            //N个小文件合并：文件名是整型，文件内容是文本
            for(int i = 1;i<= data.length;i++){
                writer.append(new IntWritable(i),
                        new Text(data[i-1]));
            }
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}