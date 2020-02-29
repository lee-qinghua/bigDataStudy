package com.qinghua.mapreduce.Demo1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 记得在configuration 的progrom arguments 里面写参数用空格隔开 第一个是输入文件args[0] 第二个是输出位置
 */
public class WcDriver {
    public static void main(String[] args) throws Exception {
        //1 获取一个job实例
        Job job = Job.getInstance();
        //2 设置类的路径
        job.setJarByClass(WcDriver.class);
        //3 设置mapper和reduce的路径
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);
        //4 设置mapper和reducer的输入输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //5 设置输入输出数据
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //6 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
