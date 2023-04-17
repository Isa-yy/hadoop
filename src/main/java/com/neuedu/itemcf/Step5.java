package com.neuedu.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * 步骤5：三维矩阵的数据相加获得所有用户对所有物品的推荐值
 *
 * @author Isa
 * @date 2023-04-10
 */
public class Step5 {
    private static class Step5Mapper extends Mapper<LongWritable, Text, Text, Text> {
        Pattern pattern = Pattern.compile("[\t,]");
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            //u2787	i100,1.0
            //u2787	i1,4.0
            //...
            String[] tokens = pattern.split(value.toString());
            Text k = new Text(tokens[0]);//用户为key
            Text v = new Text(tokens[1]+","+tokens[2]);
            context.write(k,v);
        }
    }

    private static class Step5Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            //输入：u2787	i100,1.0
            //输出：<"u2018",<"i101:3","i102:2"....>>
           Map<String,Double> map = new HashMap<String,Double>();//结果
            for(Text v:values){
                String[] tokens = v.toString().split(",");
                String itemID = tokens[0];
                Double score = Double.parseDouble(tokens[1]);
                if(map.containsKey(itemID)){
                    map.put(itemID,map.get(itemID)+score);//矩阵乘法求和计算
                }else {
                    map.put(itemID,score);
                }
            }
            Iterator<String> iter = map.keySet().iterator();
            while(iter.hasNext()){
                String itemID = iter.next();
                double score = map.get(itemID);
                Text v = new Text(itemID+","+score);
                context.write(key,v);
            }
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
            Job job = Job.getInstance(conf, "step5");
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            //设置Mapper
            job.setMapperClass(Step5Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //设置Reducer
            job.setReducerClass(Step5Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            //执行
            boolean flag = job.waitForCompletion(true);
            //提示
            if (flag) {
                System.out.println("步骤5：三维矩阵的数据相加获得所有用户对所有物品的推荐值完成~~");
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

