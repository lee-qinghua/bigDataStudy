package com.qinghua.mapreduce.自定义分区规则;

import com.qinghua.mapreduce.flowdemo.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, FlowBean> {

    /**
     * 按照手机号分区
     * 手机号136、137、138、139开头都分别放到一个独立的4个文件中，其他开头的放到一个文件中。
     * <p>
     * 注意：目前是5个分区，一定要在Driver里设置好reduceTask的数量。自定义分区数，一定要从0开始，连续的写分区数。
     *
     * @param text
     * @param flowBean
     * @param numPartitions
     * @return
     */
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        switch (text.toString().substring(0,3)) {
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}
