package com.qinghua.mapreduce.找共同好友;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.Iterator;

public class FriendReducer1 extends Reducer<Text, Text, Text, Text> {
    private Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        Iterator<Text> iterator = values.iterator();
        Text next = iterator.next();
        stringBuilder.append(next.toString());
        while (iterator.hasNext()) {
            Text next1 = iterator.next();
            stringBuilder.append(",").append(next1.toString());
        }
        v.set(stringBuilder.toString());
        context.write(key, v);
    }
}
