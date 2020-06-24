package com.qinghua.mapreduce.找共同好友;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class FriendMap2 extends Mapper<LongWritable, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //A	I,K,C,B,G,F,H,O,D这样的数据
        String[] split1 = value.toString().split("\t");
        String s = split1[0];
        v.set(s);
        String[] split2 = split1[1].split(",");
        Arrays.sort(split2);
        for (int i = 0; i < split2.length; i++) {
            if ((i + 1) < split2.length - 1) {
                for (int i1 = i + 1; i1 < split2.length; i1++) {
                    k.set(split2[i] + "-" + split2[i1]);
                    context.write(k, v);
                }
            }
        }
    }
}
