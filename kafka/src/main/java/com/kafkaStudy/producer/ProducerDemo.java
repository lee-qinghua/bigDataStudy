package com.kafkaStudy.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        /**1. 配置kafka参数*/
        Properties props = new Properties();
        //new ProducerConfig() 更方便快捷的kafka参数类
        props.put("bootstrap.servers", "hadoop102:9092");//kafka 集群，broker-list地址
        props.put("acks", "all");//ack机机制
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384); //批次大小，在等待时间内超过这个大小就会发送
        props.put("linger.ms", 1);//等待时间 ，在等待时间内没有超过批次大小，等待时间到了1ms会发送消息
        props.put("buffer.memory", 33554432); //RecordAccumulator 缓冲区大小
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");//序列化的类
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.kafkaStudy.自定义分区器.MyPartitioner");//添加自定义的分区器
        /**2. 创建生产者对象*/
        KafkaProducer kafkaProducer = new KafkaProducer<>(props);

        /**3. 发送消息*/
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("myTopic", "myMessage"); //可以指定分区
        kafkaProducer.send(record);

        /**4. 关闭连接*/
        kafkaProducer.close();

    }
}
