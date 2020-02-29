package com.qinghua.mapreduce.自定义排序;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MyFlowMapper extends Mapper<LongWritable, Text, MyFlowBean, Text> {
    private MyFlowBean bean = new MyFlowBean();
    private Text tt = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        List<String> list = Arrays.asList(s.split("\t"));
        String phone = list.get(1);
        tt.set(phone);
        Collections.reverse(list);
        if (CollectionUtils.isNotEmpty(list) && list.size() > 3) {
            long down = Long.parseLong(list.get(1));
            long up = Long.parseLong(list.get(2));
            bean.set(up, down);
        }
        context.write(bean, tt);
    }
}
