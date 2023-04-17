package com.neuedu.wc2;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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
