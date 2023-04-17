package com.neuedu.myweather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Desc4Temperature extends WritableComparator {
    public Desc4Temperature() {
        //声明一下类型
        super(YearTemperature.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        YearTemperature yt1 = (YearTemperature) a;
        YearTemperature yt2 = (YearTemperature) b;
        //年份不同，比较年份
        if(yt1.getYear() != yt2.getYear()){
            return yt1.getYear() - yt2.getYear();
        }
        //年份相同，比较温度（降序为负数）
        return -(int) (yt1.getTemperature() - yt2.getTemperature());
    }
}
