package com.neuedu.wc;

import com.neuedu.wc2_combiner.WordCountCombiner;
import org.apache.commons.lang3.StringUtils;
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
import java.util.StringTokenizer;

/**
 * mapreduce并行计算：词频统计案例
 * @author Isa
 * @create 2022-12-12
 */
public class MyWordCount {
    /**
     * 自定义Mapper类:拆分单词
     */
    private static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            //数据清洗
            String line = value.toString();
            //判断是否为空，为空不再处理
            if (StringUtils.isBlank(line)) {
                return;
            }
            //拆分单词
            StringTokenizer st = new StringTokenizer(line);
            //循环提取单词
            while (st.hasMoreTokens()) {
                //提取单词
                String word = st.nextToken();
                //输出
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    /**
     * 自定义Reducer处理类:统计单词次数
     */
    private static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            //统计单词次数
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            //输出单词次数
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) {
        try {
            //本地操作
            //构建配置对象
            Configuration conf = new Configuration();
            //设置HADOOP集群属性,若提供core-site.xml时，无需设置，自动读取
            //conf.set("fs.defaultFS","hdfs://master:9000");
            //获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义输入目录
            String input = "/books";
            //定义输出目录
            String output = "/wc_output";
            Path outputPath = new Path(output);
            //判断输出目录是否存在，存在则删除
            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }
            //实例化Job对象
            Job job = Job.getInstance(conf, "word count");
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            //设置Mapper
            job.setMapperClass(WordCountMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            //设置归并
            job.setCombinerClass(WordCountCombiner.class);
            //设置Reducer
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            //设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            //执行
            boolean flag = job.waitForCompletion(true);
            //提示
            if (flag) {
                System.out.println("词频统计结束~~~~");
            }
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
