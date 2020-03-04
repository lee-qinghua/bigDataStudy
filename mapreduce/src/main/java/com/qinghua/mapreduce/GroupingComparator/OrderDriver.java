package com.qinghua.mapreduce.GroupingComparator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderDriver {
    public static void main(String[] args) throws Exception {

        //1 获取一个job实例
        Job job = Job.getInstance();
        //2 设置类的路径
        job.setJarByClass(OrderDriver.class);
        //3 设置mapper和reduce的路径
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReduce.class);
        //4 设置mapper和reducer的输入输出格式
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        //5 设置输入输出数据
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置compartor
        job.setGroupingComparatorClass(OrderComparator.class);

        //6 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
