package com.neuedu.myweather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 自定义类
 *
 * @author Isa
 * @date 2023-04-03
 */
public class YearTemperature implements WritableComparable<YearTemperature> {
    //1、定义属性
    private int year;
    private double temperature;

    //2、定义构造器

    public YearTemperature() {
    }

    public YearTemperature(int year, double temperature) {
        this.year = year;
        this.temperature = temperature;
    }
    //3、重写toString方法，也就是输出

    @Override
    public String toString() {
        return  year + "\t" + temperature ;
    }

    //4、实现接口
    @Override
    public int compareTo(YearTemperature other) {
        //默认排序、默认比较：返回值>0是大，=0是相等，<0是小
        if(other == null){
            return 1;
        }
        //年份不同，比较年份
        if(this.year != other.year){
            //return Integer.compare(this.year,other.year);
            return this.year-other.year;
        }
        //年份相同，比较温度
        //return Double.compare(this.temperature,other.temperature);
        return (int)(this.temperature-other.temperature);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //hadoop 序列化
        dataOutput.writeInt(this.year);
        dataOutput.writeDouble(this.temperature);
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //hadoop反序列化
        this.year = dataInput.readInt();
        this.temperature = dataInput.readDouble();
    }

    //5.定义存取器

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }
}
