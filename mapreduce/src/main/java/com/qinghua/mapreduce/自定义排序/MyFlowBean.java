package com.qinghua.mapreduce.自定义排序;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyFlowBean implements WritableComparable<MyFlowBean> {
    private long up;
    private long down;
    private long sum;

    public MyFlowBean() {
    }

    public void set(long up, long down) {
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }

    public Long getUp() {
        return up;
    }

    public void setUp(Long up) {
        this.up = up;
    }

    public Long getDown() {
        return down;
    }

    public void setDown(Long down) {
        this.down = down;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    /**
     * 序列化
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    /**
     * 反序列化
     * 顺序一定要和序列化时的顺序一致
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        up = in.readLong();
        down = in.readLong();
        sum = in.readLong();
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "up=" + up +
                ", down=" + down +
                ", sum=" + sum +
                '}';
    }

    /**
     * 返回0相等
     *
     * @param o
     * @return
     */
    public int compareTo(MyFlowBean o) {
        return Long.compare(o.sum,sum);
        //按照总流量倒序排序
    }
}
