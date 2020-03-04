package com.qinghua.mapreduce.join的应用.mapJoin;

import com.qinghua.mapreduce.join的应用.reduceJoin.OrderBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapJoinOrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean orderBean = new OrderBean();
    private IntWritable intWritable = new IntWritable();
    Map<String, String> pdMap = new HashMap<>();

    /**
     * 获取缓存文件
     */
    protected void setup(Context context) throws IOException {
        // 1 获取缓存的文件
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            // 2 切割
            String[] fields = line.split("\t");
            // 3 缓存数据到集合
            pdMap.put(fields[0], fields[1]);
        }
        // 4 关流
        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //要根据不同的文件名设置不同的参数，就需要知道此时正在读取的是那个文件
        String values = value.toString();
        String[] fields = values.split("\t");
        orderBean.setId(fields[0]);
        orderBean.setPid(fields[1]);
        orderBean.setAmount(Integer.parseInt(fields[2]));
        orderBean.setPname(pdMap.get(fields[1]));
        context.write(orderBean, NullWritable.get());
    }
}
