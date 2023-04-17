package com.neuedu.smallfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
public class M1W {
    public static void main(String[] args) {
        //定义文件内容：模拟4个文件
        String[] data = {"hhhhhhh uuuuuuu",
                "llllll uuuuuu",
                "qqqqqqqq iiiiiii",
                "oooooooooo wwwww"};
        try {
            //构建配置对象
            Configuration conf = new Configuration();
            //定义远程文件位置
            Path dst = new Path("/mapfile/m");
            //定义写入器
            MapFile.Writer writer = new MapFile.Writer(conf,dst,
                    MapFile.Writer.keyClass(IntWritable.class),
                    MapFile.Writer.valueClass(Text.class),
                    MapFile.Writer.compression(SequenceFile.CompressionType.NONE));
            //N个小文件合并
            for(int i =1; i<= data.length; i++){
                writer.append(new IntWritable(i),new Text(data[i-1]));
            }
            //关闭
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}