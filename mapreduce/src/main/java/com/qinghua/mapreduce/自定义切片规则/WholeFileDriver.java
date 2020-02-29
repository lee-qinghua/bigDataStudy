package com.qinghua.mapreduce.自定义切片规则;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class WholeFileDriver {
    public static void main(String[] args) throws Exception {
        //1 获取一个job实例
        Job job = Job.getInstance();
        //2 设置类的路径
        job.setJarByClass(WholeFileDriver.class);
        //3 设置mapper和reduce的路径 不设置会走默认的

        //4 设置mapper和reducer的输入输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        //5 设置输入输出数据
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputValueClass(SequenceFileOutputFormat.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //6 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
