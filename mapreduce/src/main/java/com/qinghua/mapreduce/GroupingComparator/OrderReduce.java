package com.qinghua.mapreduce.GroupingComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderReduce extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        Iterator<NullWritable> iterator = values.iterator();
        for (int i = 0; i < 2; i++) {
            if (iterator.hasNext()) {
                iterator.next();
                context.write(key, NullWritable.get());
            }
        }
    }
}
