package com.neuedu.wc2_group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGrouping extends WritableComparator {
    public MyGrouping(){
        super(Text.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //自定义分组：仅关注是否为0
        Text t1 = (Text) a;
        Text t2 = (Text) b;
        return t1.compareTo(t2);
    }
}
