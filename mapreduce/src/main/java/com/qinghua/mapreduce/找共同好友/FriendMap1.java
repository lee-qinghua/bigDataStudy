package com.qinghua.mapreduce.找共同好友;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendMap1 extends Mapper<LongWritable, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");
        String person = split[0];
        v.set(person);
        String[] friends = split[1].split(",");
        for (String friend : friends) {
            k.set(friend);
            context.write(k, v);
        }
    }
}
