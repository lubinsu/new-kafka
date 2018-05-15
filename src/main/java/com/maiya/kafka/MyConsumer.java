package com.maiya.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Created by lubinsu
 * Date: 2018/5/14 11:30
 * Desc: Consumer示例
 */
public class MyConsumer {

    public static void AutomaticOffsetCommitting() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.181:9092,192.168.0.182:9092,192.168.0.183:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("my-topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

    public static void ManualOffsetControl() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.181:9092,192.168.0.182:9092,192.168.0.183:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("my-topic"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        /*while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                buffer.forEach(record -> {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                });

                consumer.commitSync();
                buffer.clear();
            }
        }*/

        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println(record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    /**
                     * 提交到指定的偏移位置
                     */
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        ManualOffsetControl();
    }
}
