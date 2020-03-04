package com.qinghua.mapreduce.join的应用.reduceJoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private String fileName;
    private OrderBean orderBean = new OrderBean();
    private IntWritable intWritable = new IntWritable();

    /**
     * 初始化的方法
     * 获取文件名
     */
    protected void setup(Context context) {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        fileName = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //要根据不同的文件名设置不同的参数，就需要知道此时正在读取的是那个文件
        String values = value.toString();
        String[] fields = values.split("\t");
        if (fileName.contains("order")) {//订单表
            orderBean.setId(fields[0]);
            orderBean.setPid(fields[1]);
            orderBean.setAmount(Integer.parseInt(fields[2]));
            orderBean.setPname("");//todo 虽然没有值但是一定要补全bean，这里一定要设置一个空值，因为hadoop的序列化和反序列化的原因，不然会报错。不像java的类一样。
        } else {//产品表
            // 2.4 封装bean对象
            orderBean.setPname(fields[1]);
            orderBean.setPid(fields[0]);
            orderBean.setAmount(0);//todo 虽然没有值但是一定要补全bean，这里一定要设置一个空值，因为hadoop的序列化和反序列化的原因，不然会报错。不像java的类一样。
            orderBean.setId("");//todo 虽然没有值但是一定要补全bean，这里一定要设置一个空值，因为hadoop的序列化和反序列化的原因，不然会报错。不像java的类一样。
        }
        context.write(orderBean, NullWritable.get());
    }
}
