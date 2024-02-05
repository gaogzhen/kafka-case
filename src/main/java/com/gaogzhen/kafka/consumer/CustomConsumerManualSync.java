package com.gaogzhen.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author: Administrator
 * @createTime: 2024/02/03 10:06
 */
public class CustomConsumerManualSync {
    public static void main(String[] args) {
        // 1 配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 配置消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test001");

        // 手动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 2 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 3 定义主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        consumer.subscribe(topics);
        // 4 消费数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }

            // 手动提交offset:sync,async
            // consumer.commitSync();
            consumer.commitAsync();

        }

    }
}
