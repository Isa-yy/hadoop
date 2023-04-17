package com.neuedu.myweather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组：按年份分组
 *
 * @author Isa
 * @date 2023-04-03
 */
public class MyGrouping extends WritableComparator {
    public MyGrouping() {
        //声明一下类型
        super(YearTemperature.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        YearTemperature yt1 = (YearTemperature) a;
        YearTemperature yt2 = (YearTemperature) b;
        //年份相比
        return yt1.getYear() - yt2.getYear();

    }
}
