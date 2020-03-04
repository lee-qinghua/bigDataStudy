package com.qinghua.mapreduce.GroupingComparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private OrderBean orderBean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        orderBean.setOrderId(split[0]);
        orderBean.setPid(split[1]);
        orderBean.setPrice((long) Double.parseDouble(split[2]));
        context.write(orderBean, NullWritable.get());
    }
}
