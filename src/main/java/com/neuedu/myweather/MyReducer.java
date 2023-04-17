package com.neuedu.myweather;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Isa
 * @date 2023-04-03
 */
public class MyReducer extends Reducer<YearTemperature,Text,YearTemperature, NullWritable> {
    @Override
    protected void reduce(YearTemperature key, Iterable<Text> values, Reducer<YearTemperature, Text, YearTemperature, NullWritable>.Context context) throws IOException, InterruptedException {
        //接受的数据
        //<[1949 34],['1949-10-01 14:21:02	34℃','1949-10-01 14:21:02	34℃',....]>
        for(Text v:values){
            //输出
            context.write(key,NullWritable.get());
            break;
        }
    }
}
