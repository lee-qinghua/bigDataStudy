package com.qinghua.mapreduce.用combiner;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * combiner的输出就是reducer的输入，所以不能改kv的格式
 */
public class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable intWritable = new IntWritable();

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count++;
        }
        intWritable.set(count);
        context.write(key, intWritable);
    }
}
