package com.qinghua.mapreduce.Demo1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text text = new Text();
    IntWritable intWritable = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //把输入的value转成字符串 用空格分开 输出成（单词,1）的格式
        String s = value.toString();
        String[] words = s.split(" ");
        for (String word : words) {
            text.set(word);
            intWritable.set(1);
            context.write(text, intWritable);
        }
    }
}
