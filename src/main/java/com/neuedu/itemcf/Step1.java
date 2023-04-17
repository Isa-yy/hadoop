package com.neuedu.itemcf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


/**
 * 步骤1：数据清洗与导入
 *
 * @author Isa
 * @date 2023-04-10
 */
public class Step1 {
    private static class Step1Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            //i1,u2735,click,2014-9-3 16:23
            //判断是否为空
            String line = value.toString();
            if(StringUtils.isBlank(line)){
                return;
            }
            //拆分
            //判断数据是否合法：完整性
            String[] items = line.split(",",4);
            if(items.length != 4){
                return;
            }
            //输出
            context.write(value,NullWritable.get());
        }
    }
    private static class Step1Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            //清理数据：去掉重复项（由于mapper传输时 键唯一，所以自然没有重复）
            //直接输出
            context.write(key,NullWritable.get());
        }
    }
    public static void run(String input, String output) {
        try {
            //本地操作
            //构建配置对象
            Configuration conf = new Configuration();
            //设置HADOOP集群属性,若提供core-site.xml时，无需设置，自动读取
            //conf.set("fs.defaultFS","hdfs://master:9000");
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义输入目录
            //String input = "/books";
            //定义输出目录
            //String output = "/wc_output";
            Path outputPath = new Path(output);
            //判断输出目录是否存在，存在则删除
            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }
            //实例化Job对象
            Job job = Job.getInstance(conf, "step1");
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            //设置Mapper
            job.setMapperClass(Step1Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            //设置Reducer
            job.setReducerClass(Step1Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            //设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            //执行
            boolean flag = job.waitForCompletion(true);
            //提示
            if (flag) {
                System.out.println("步骤1：数据清洗与导入完成~~");
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
