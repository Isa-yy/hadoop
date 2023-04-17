package com.neuedu.itemcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;


/**
 * 步骤6：按照推荐值降序排列
 *
 * @author Isa
 * @date 2023-04-10
 */
public class Step6 {
    //1、定义属性
    //2、定义构造器
    //3、重写toString方法，也就是输出
    //4、实现接口
    //5.定义存取器

    /**
     * 自定义比较型
     */
    private  static class PairWritable implements WritableComparable<PairWritable>{
        //1、定义属性
        private String uid;
        private double num;
        //2、定义构造器
        public PairWritable() {
        }
        public PairWritable(String uid, double num) {
            super();
            this.uid = uid;
            this.num = num;
        }
        //3、重写toString方法，也就是输出

        @Override
        public String toString() {
            return uid+"\t"+num;
        }

        //4、实现接口
        @Override
        public int compareTo(PairWritable o) {
            int r = this.uid.compareTo(o.uid);
            if(r==0){
                return Double.compare(this.num,o.num);
            }
            return r;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(this.uid);
            dataOutput.writeDouble(this.num);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.uid = dataInput.readUTF();
            this.num = dataInput.readDouble();
        }
        //5.定义存取器

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public double getNum() {
            return num;
        }

        public void setNum(double num) {
            this.num = num;
        }
    }
    private static class Step6Mapper extends Mapper<LongWritable, Text, PairWritable,Text> {
        Pattern pattern = Pattern.compile("[\t,]");
        private  final static Text K = new Text();
        private  final static Text V = new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PairWritable, Text>.Context context) throws IOException, InterruptedException {
            //u2723	i1,1.0
            //u2723	i100,1.0
            String[] tokens = pattern.split(value.toString());
            String userID = tokens[0];
            String itemID = tokens[1];
            String num = tokens[2];
            PairWritable k = new PairWritable();
            k.setUid(userID);
            k.setNum(Double.parseDouble(num));
            V.set(itemID +":"+ num);
            System.out.println(k +"\t"+V);
            context.write(k,V);
        }
    }
    private static class NumSort extends WritableComparator {
        public NumSort() {
            //声明一下类型
            super(PairWritable.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            PairWritable p1 = (PairWritable) a;
            PairWritable p2 = (PairWritable) b;
            //用户不同，比较用户
            int r = p1.getUid().compareTo(p2.getUid());
            if(r==0){
                //用户相同，比较物品（降序为负数）
                return -Double.compare(p1.getNum(),p2.getNum());
            }
            return r;
        }
    }
    public static class UserGroup extends WritableComparator {
        public UserGroup() {
            //声明一下类型
            super(PairWritable.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            PairWritable p1 = (PairWritable) a;
            PairWritable p2 = (PairWritable) b;
            //用户相比
            return p1.getUid().compareTo(p2.getUid());
        }
    }
    private static class Step6Reducer extends Reducer<PairWritable,Text, Text,Text> {
        private  final static Text K = new Text();
        private  final static Text V = new Text();
        @Override
        protected void reduce(PairWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            int i =0;
            StringBuffer sb =new StringBuffer();
            for(Text v:values){
                if(i==10) {//推荐前十的物品
                    break;
                }
                sb.append(v.toString()).append(",");
                i++;
            }
            System.out.println(i);
            //删除最后的逗号
            sb.deleteCharAt(sb.length()-1);
            K.set(key.getUid());
            V.set(sb.toString());
            context.write(K,V);
            System.out.println(K+"\t"+V);
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
            Job job = Job.getInstance(conf, "step6");
            //设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            //设置Mapper
            job.setMapperClass(Step6Mapper.class);
            job.setMapOutputKeyClass(PairWritable.class);
            job.setMapOutputValueClass(Text.class);
            //*设置排序
            job.setSortComparatorClass(NumSort.class);
            //*设置分组
            job.setGroupingComparatorClass(UserGroup.class);
            //设置Reducer
            job.setReducerClass(Step6Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            //执行
            boolean flag = job.waitForCompletion(true);
            //提示
            if (flag) {
                System.out.println("步骤6：按照推荐值降序排列完成~~");
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
