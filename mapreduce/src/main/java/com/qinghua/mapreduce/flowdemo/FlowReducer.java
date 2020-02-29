package com.qinghua.mapreduce.flowdemo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean bean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int up = 0;
        int down = 0;
        for (FlowBean value : values) {
            up += value.getUp();
            down += value.getDown();
        }
        bean.set(up, down);
        context.write(key, bean);
    }
}
