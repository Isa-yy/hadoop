package com.neuedu.myweather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author Isa
 */
public class Run {
    public static void main(String[] args) {
        try {
            //构建配置对象
            Configuration conf = new Configuration();
            //设置HADOOP集群属性,若提供core-site.xml时，无需设置，自动读取
            //conf.set("fs.defaultFS","hdfs://master:9000");
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义输入目录
            String input = "/myweather";
            //使用参数定义输入目录
            //String input = args[0];
            //定义输出目录
            String output = "/myweather_output";
            //使用参数定义输出目录
            //String output = args[1];
            Path outputPath = new Path(output);
            //判断输出目录是否存在，存在则删除
            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }
            //实例化Job对象
            Job job = Job.getInstance(conf, "max temperature of year");
            //设置运行类
            job.setJarByClass(Run.class);
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            //设置Mapper
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(YearTemperature.class);
            job.setMapOutputValueClass(Text.class);
            //*设置分区
            job.setNumReduceTasks(3);
            job.setPartitionerClass(MyPartitioner.class);
            //*设置排序
            job.setSortComparatorClass(Desc4Temperature.class);
            //*设置分组
            job.setGroupingComparatorClass(MyGrouping.class);
            //设置Reducer
            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(YearTemperature.class);
            job.setOutputValueClass(NullWritable.class);
            //设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            //执行
            boolean flag = job.waitForCompletion(true);
            //提示
            if (flag) {
                System.out.println("每年最高温度统计结束~~~~");
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}

