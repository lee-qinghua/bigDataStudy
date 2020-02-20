package com.kafkaStudy.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        //消费者配置信息
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");//连接的集群信息
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");//自动提交的延时
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);//自动提交  如果是false会重复消费消息
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "myConsumerGroup");//指定消费者组
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "");//重置offset

        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        ArrayList<String> list = new ArrayList<>();
        list.add("topic1");
        list.add("topic2");
        consumer.subscribe(list);//消费者订阅主题

        //循环不停的拉取消息
        while (true) {
            //由于kafka获取消息是一直论循拉取的，当拉取不到信息时（没有信息发送），会等待一段时间后再进行拉取
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key());
                System.out.println(consumerRecord.value());
            }
        }
    }
}
