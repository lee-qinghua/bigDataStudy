package com.qinghua.mapreduce.自定义排序;

import com.qinghua.mapreduce.flowdemo.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyFlowReducer extends Reducer<MyFlowBean, Text, Text, MyFlowBean> {
    private FlowBean bean = new FlowBean();

    @Override
    protected void reduce(MyFlowBean bean, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(value, bean);
        }
    }
}
