package com.neuedu.itemcf;

import org.apache.commons.lang3.StringUtils;
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
import java.util.Map;


/**
 * 步骤2：获取所有用户的喜欢矩阵
 *
 * @author Isa
 * @date 2023-04-10
 */
public class Step2 {
    private static class Step2Mapper extends Mapper<LongWritable, Text, Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
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
            //物品编号
            String itemID = items[0];
            //用户编号
            String userID = items[1];
            //行为
            String action = items[2];
            //喜爱程度，也就是权重
            Integer v = Manager.getAction().get(action);
            //组合结构：物品编号 ：喜爱程度
            String item_value = itemID + ":" + v;
            //输出
            context.write(new Text(userID),new Text(item_value));
        }
    }
    private static class Step2Reducer extends Reducer<Text,Text, Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            //输入：<"u2018",<"i101:1","i101:2","i102:2"....>>
            //输出：<"u2018",<"i101:3","i102:2"....>>
            //输出指定用户的所有物品的喜爱程度：考虑物品
            //<"i101:1","i101:2","i102:2"....>本身为一个键值对
            Map<String,Integer> items = new HashMap<>();
            for(Text v:values){
                System.out.println(v);
                String[] iv=v.toString().split(":");
                String n = iv[0];
                Integer k = Integer.valueOf(iv[1]);
                //没有此商品赋为0，其余将权重（item的value）相加
                k += items.get(n) == null ? 0 : items.get(n);
                items.put(n,k);
            }
            //将所有物品及喜爱程度拼成字符串
            StringBuffer sb = new StringBuffer();
            for(Map.Entry<String,Integer> kv: items.entrySet()){
                sb.append(kv.getKey()+":"+kv.getValue()+",");
            }
            //删除最后的逗号
            sb.deleteCharAt(sb.length()-1);
            //输出<用户,所有物品及喜爱程度
            //输出：<"u2018",<"i101:3","i102:2"....>>
            context.write(key,new Text(sb.toString()));
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
            Job job = Job.getInstance(conf, "step2");
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            //设置Mapper
            job.setMapperClass(Step2Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //设置Reducer
            job.setReducerClass(Step2Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            //执行
            boolean flag = job.waitForCompletion(true);
            //提示
            if (flag) {
                System.out.println("步骤2：获取所有的用户的喜欢矩阵完成~~");
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
