package com.qinghua.mapreduce.用combiner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *用combiner减小网络传输，
 * 在控制台打印的 Map output materialized bytes=57 会发现这个值小了
 */
public class MyWcDriver {
    public static void main(String[] args) throws Exception {
        //1 获取一个job实例
        Job job = Job.getInstance();
        //2 设置类的路径
        job.setJarByClass(MyWcDriver.class);
        //3 设置mapper和reduce的路径
        job.setMapperClass(MyWcMapper.class);
        job.setReducerClass(MyWcReducer.class);
        //4 设置mapper和reducer的输入输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //5 设置输入输出数据
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //设置combiner
        //job.setCombinerClass(MyCombiner.class);

        //6 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
