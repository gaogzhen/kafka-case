package com.gaogzhen.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author: Administrator
 * @createTime: 2024/01/27 10:32
 */
public class CustomProducerTransaction {
    public static void main(String[] args) {
        // 1. 配置
        Properties properties = new Properties();
        // 1.1连接配置
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092,node3:9092");
        // 1.2指定key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置事务id，唯一
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional_id_002");
        // 1.创建生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        kafkaProducer.initTransactions();
        kafkaProducer.beginTransaction();
        // 2.发送数据
        try {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>("first", "zookeeper2" + i));
            }
            int i = 1/0;
            kafkaProducer.commitTransaction();
        }catch (Exception e) {
            kafkaProducer.abortTransaction();
        } finally {
            // 3.关闭资源
            kafkaProducer.close();

        }
    }
}
