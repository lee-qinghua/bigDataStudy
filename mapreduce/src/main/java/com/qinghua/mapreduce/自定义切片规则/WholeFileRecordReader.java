package com.qinghua.mapreduce.自定义切片规则;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * The record reader breaks the data into key/value pairs for input to the
 * recordReader是把数据转换成key-value的格式
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private FileSplit file;
    private Configuration configuration;
    private Text k = new Text();
    private BytesWritable v = new BytesWritable();
    private boolean flage = true;

    /**
     * 初始化调用的方法
     *
     * @param inputSplit
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        file = (FileSplit) inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    /**
     * 读取下一个键值对 ，类似于一行一行的数据，读完一行读下一行，如果返回true则还有数据，如果返回false则读完了
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (flage) {
            FSDataInputStream fis = null;
            try {
                byte[] buf = new byte[(int) file.getLength()];
                Path path = file.getPath();
                FileSystem fileSystem = path.getFileSystem(configuration);
                fis = fileSystem.open(path);//打开输入流
                IOUtils.readFully(fis, buf, 0, buf.length);
                v.set(buf, 0, buf.length);
                k.set(path.toString());
                flage = false;
                return true;
            } finally {
                IOUtils.closeStream(fis);
            }
        }
        return false;
    }

    /**
     * 读取文件时，获取正在读取当前的key
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    /**
     * 读取文件时，获取正在读取当前的value
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    /**
     * 获取当前读取的进度
     * a number between 0.0 and 1.0 that is the fraction of the data read
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public float getProgress() throws IOException, InterruptedException {
        return flage ? 0 : 1;
    }

    /**
     * 最后处理
     *
     * @throws IOException
     */
    public void close() throws IOException {

    }
}
