package com.kafkaStudy.自定义消息拦截器;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义的消息拦截器
 * 创建生产者属性添加，可以放多个拦截器，放到list中
 * props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,"com.kafkaStudy.自定义消息拦截器.MyInterceptor");//添加自定义的消息拦截器
 * <p>
 * 1. 发送消息时给消息加上时间戳
 * 2. 对消息发送成功和失败的条数进行计数
 */
public class MyInterceptor implements ProducerInterceptor<String, String> {
    int success;
    int error;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        String value = producerRecord.value();//获取到消息
        return new ProducerRecord<String, String>(producerRecord.topic(), producerRecord.partition(), producerRecord.key(), System.currentTimeMillis() + producerRecord.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {//说明消息发送成功
            success++;
        } else {
            error++;
        }
    }

    @Override
    public void close() {
        //在这里记录或者打印成功和失败的条数
        System.out.println(success);
        System.out.println(error);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
