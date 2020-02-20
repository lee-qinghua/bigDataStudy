package com.kafkaStudy.自定义分区器;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义的分区器
 * 在生产者配置属性类中指定自定义的分区器
 * props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.kafkaStudy.自定义分区器.MyPartitioner");//添加自定义的分区器
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //重写这个方法 可以根据传过来的key来进行分区,key可以是消息的特征（比如用户的手机号码或者userid）,根据key的hash值进行分区
        //org.apache.kafka.clients.producer.internals.DefaultPartitioner 默认的分区器 可以查看他的代码

        //得到这个topic所有的分区的数量
        Integer count = cluster.partitionCountForTopic(topic);
        //List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);//获取现在正常运行的分区
        return key.hashCode() % count; //具体业务具体分析
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
