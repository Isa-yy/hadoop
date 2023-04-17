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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * 步骤4：同现矩阵与喜欢矩阵相乘得到三维矩阵
 *
 * @author Isa
 * @date 2023-04-10
 */
public class Step4 {
    protected static class Step4Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private String flag;//是 A同现矩阵 或 B得分矩阵

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            //判断读取的数据
            flag = split.getPath().getParent().getName();
            System.out.println(flag);
        }
        Pattern pattern = Pattern.compile("[\t,]");
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            //输入:同现矩阵：i100:i1	1
            //            i100:i100	3
            //                  ...
            //    得分矩阵：u2723	i101:1,i102:1
            //            u2729	i10:4
            //                  ...
            String[] tokens = pattern.split(value.toString());
            if(flag.equals("step3_output")){//同现矩阵
                //i100:i1	1
                String[] v1 = tokens[0].split(":");
                String itemA = v1[0];//i100
                String itemB = v1[1];//i1
                String num = tokens[1];
                Text k = new Text(itemA);//第一个物品为key i100
                Text v = new Text("A:"+itemB+","+num);//Ai1,1
                System.out.println(k+"\t"+v);
                context.write(k,v);
            } else if (flag.equals("step2_output")) {//所有用户的喜欢矩阵
                //u2723	i101:1,i102:1
                String userID = tokens[0];//u2723
                for(int i=1;i< tokens.length;i++){//i101:1,i102:1
                    String[] vector = tokens[i].split(":");
                    String itemID = vector[0];//物品编号 i101
                    String pref = vector[1];//喜爱分数 1
                    Text k = new Text(itemID);
                    Text v = new Text("B:"+userID+","+pref);
                    System.out.println(k+"\t"+v);
                    context.write(k,v);
                }

            }
        }

    }
    private static class Step4Reducer extends Reducer<Text, Text, Text, Text> {
        Pattern pattern = Pattern.compile("[\t,]");
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Map<String,Integer> mapA = new HashMap<String,Integer>();
            Map<String,Integer> mapB = new HashMap<String,Integer>();
            for(Text v:values){
                String val = v.toString();
                if(val.startsWith("A:")){//表示物体同现矩阵
                    String[] kv = pattern.split(val.substring(2));
                    try{
                        mapA.put(kv[0],Integer.parseInt(kv[1]));
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                } else if (val.startsWith("B:")){//用户喜欢矩阵
                    String[] kv = pattern.split(val.substring(2));
                    try{
                        mapB.put(kv[0],Integer.parseInt(kv[1]));
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }
            double result = 0;
            Iterator<String> iter = mapA.keySet().iterator();
            while(iter.hasNext()){
                String mapk = iter.next();//itemID物品编号
                int num = mapA.get(mapk).intValue();
                Iterator<String> iterb = mapB.keySet().iterator();
                while(iterb.hasNext()){
                    String mapkb = iterb.next();
                    int pref = mapB.get(mapkb).intValue();
                    result = num * pref;//矩阵乘法计算
                    Text k = new Text(mapkb);
                    Text v = new Text(mapk+","+result);
                    System.out.println(k+"\t"+v);
                    context.write(k,v);
                }
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
            Job job = Job.getInstance(conf, "step4");
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, new Path("/itemcf/step2_output"),new Path("/itemcf/step3_output"));
            //设置Mapper
            job.setMapperClass(Step4Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //设置Reducer
            job.setReducerClass(Step4Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            //执行
            boolean flag = job.waitForCompletion(true);
            //提示
            if (flag) {
                System.out.println("步骤4：同现矩阵与喜欢矩阵相乘完成~~");
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    }