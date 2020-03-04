package com.qinghua.mapreduce.join的应用.mapJoin;

import com.qinghua.mapreduce.join的应用.reduceJoin.OrderBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * 不适用reduce 减少数据偏移，省去了shuffle阶段节省很大性能，适用于一张表很小的情况下，可以把数据放到缓存中
 */
public class MapJoinOrderDriver {
    public static void main(String[] args) throws Exception {
        // 0 根据自己电脑路径重新配置
        args = new String[]{"d:/testinput/order.txt", "d:/output1"};

        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 指定本程序的jar包所在的本地路径
        job.setJarByClass(MapJoinOrderDriver.class);

        // 3 指定本业务job要使用的Mapper/Reducer业务类
        job.setMapperClass(MapJoinOrderMapper.class);

        // 4 指定Mapper输出数据的kv类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5 指定最终输出的数据的kv类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //todo ===================新增参数
        // 6 加载缓存数据
        // 7 设置reducetask的梳理为0
        job.addCacheFile(new URI("file:///d:/testinput/pd.txt"));

        // 7 Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);

        // 6 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
