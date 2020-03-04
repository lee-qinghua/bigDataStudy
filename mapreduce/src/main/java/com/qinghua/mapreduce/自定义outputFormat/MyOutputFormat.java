package com.qinghua.mapreduce.自定义outputFormat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {

        MyRecordWriter myRecordWriter = new MyRecordWriter();
        myRecordWriter.FilterRecordWriter(job);
        // 创建一个RecordWriter
        return myRecordWriter;
    }
}
