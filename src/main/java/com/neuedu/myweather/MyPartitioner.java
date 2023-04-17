package com.neuedu.myweather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区：根据年份分区
 *
 * @author Isa
 * @date 2023-4-03
 */
public class MyPartitioner extends Partitioner<YearTemperature, Text> {

    @Override
    public int getPartition(YearTemperature key, Text value, int numPartitions) {
        //分区算法
        //1949-1940 = 9 % 3=0
        //1950-1940 = 10 % 3=1
        //1951-1940 = 11 % 3=2
        return (key.getYear()-1940)%numPartitions;
    }

}
