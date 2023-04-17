package com.neuedu.myweather;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义Mapper类：负责拆分，找出年份和温度
 * @author Isa
 * @date 2023-04-05
 */

public class MyMapper extends Mapper<LongWritable, Text,YearTemperature,Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, YearTemperature, Text>.Context context) throws IOException, InterruptedException {
        //原始数据：1949-10-01 14:21:02	34℃
        String line = value.toString();
        //判断合法性
        if(StringUtils.isBlank(line)){
            return;
        }
        //拆分
        String[] items = line.split("\t");
        //判断合法性
        if(items.length != 2){
            return;
        }
        //提取年份
        int year = Integer.parseInt(items[0].substring(0,4));
        //提取温度
        double temperature = Double.parseDouble(items[1].substring(0,items[1].lastIndexOf("℃")));
        //实例化对象
        YearTemperature wt = new YearTemperature(year,temperature);
        //输出
        context.write(wt,value);
    }
}
