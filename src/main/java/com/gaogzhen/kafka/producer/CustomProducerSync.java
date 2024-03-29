package com.gaogzhen.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author: Administrator
 * @createTime: 2024/01/27 10:32
 */
public class CustomProducerSync {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 1. 配置
        Properties properties = new Properties();
        // 1.1连接配置
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        // 1.2指定key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 1.创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        // 2.发送数据
        for (int i = 0; i < 5; i++) {
            RecordMetadata metadata = kafkaProducer.send(new ProducerRecord<>("first", "zookeeper" + i)).get();
            System.out.println(metadata);
        }
        // 3.关闭资源
        kafkaProducer.close();
    }
}
