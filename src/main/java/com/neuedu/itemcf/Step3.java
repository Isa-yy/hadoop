package com.neuedu.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
 * 步骤3：获得所有物品之间的同现矩阵
 *
 * @author Isa
 * @date 2023-04-10
 */
public class Step3 {
    private static class Step3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private  final static Text K = new Text();
        private  final  static IntWritable V = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            //输入:"u2018" <"i101:3","i102:2"....>
            String[] tokens = value.toString().split("\t");
            String[] items = tokens[1].split(",");
            //->i101:3
            //  i102:2
            //  ...
            for(int i=0;i< items.length;i++){
                //i101
                String itemA = items[i].split(":")[0];
                for(int j=0; j< items.length;j++){
                    //i102
                    String itemB = items[j].split(":")[0];
                    K.set(itemA+":"+itemB);
                    //"i101:i102"
                    System.out.println(K);
                    context.write(K,V);
                }
            }
        }
    }
    private static class Step3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            //所有用户购买同时购买A,B两种商品的次数
            int sum = 0;
            for(IntWritable v:values){
                sum = sum + v.get();
            }
            Step3Mapper.V.set(sum);
            context.write(key, Step3Mapper.V);
            //输出：i100:i100	3
            //     i101:i101	2
            //     ...
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
            Job job = Job.getInstance(conf, "step3");
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            //设置Mapper
            job.setMapperClass(Step3Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            //设置Reducer
            job.setReducerClass(Step3Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            //设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            //执行
            boolean flag = job.waitForCompletion(true);
            //提示
            if (flag) {
                System.out.println("* 步骤3：获得所有物品之间的同现矩阵完成~~");
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
