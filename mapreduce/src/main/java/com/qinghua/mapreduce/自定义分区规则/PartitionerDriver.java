package com.qinghua.mapreduce.自定义分区规则;

import com.qinghua.mapreduce.flowdemo.FlowBean;
import com.qinghua.mapreduce.flowdemo.FlowMapper;
import com.qinghua.mapreduce.flowdemo.FlowReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartitionerDriver {
    public static void main(String[] args) throws Exception {
        //1 获取一个job实例
        Job job = Job.getInstance();
        //2 设置类的路径
        job.setJarByClass(PartitionerDriver.class);
        //3 设置mapper和reduce的路径
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //4 设置mapper和reducer的输入输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //5 设置输入输出数据
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置分区规则和分区数
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(5);

        //6 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
