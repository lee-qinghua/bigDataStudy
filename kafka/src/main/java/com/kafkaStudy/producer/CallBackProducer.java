package com.kafkaStudy.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 带回调参数的生产者写法
 * <p>
 * 其中配置文件只写了三个重要的参数，别的都有默认值
 */
public class CallBackProducer {
    public static void main(String[] args) {
        //配置文件
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "");//zookeeper地址
        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //发送消息带回调
        producer.send(new ProducerRecord<String, String>("myTopic", "myMessage"), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                //这个回调接口只有一个方法，如果成功返回metadata 如果失败返回exception
                if (exception == null) {
                    System.out.println(metadata.offset());//记录偏移量
                    System.out.println(metadata.partition());//记录分区
                }
            }
        });
        //关闭链接
        producer.close();
    }
}
