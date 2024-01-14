package simo.kafkaimp3;

import java.util.Arrays;
import java.util.Properties;
import java.time.Duration;

import org.apache.kafka.clients.consumer.*;

public class KafkaConsumerWrapper {
    KafkaConsumer<String, String> consumer;

    public KafkaConsumerWrapper() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9094,localhost:9096");
        props.put("group.id", "1");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        this.consumer = new KafkaConsumer<>(props);
    }

    public void subscribeAndPrint() {
        consumer.subscribe(Arrays.asList("imp3"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
    public static void main(String[] args) {
        KafkaConsumerWrapper c = new KafkaConsumerWrapper();
        c.subscribeAndPrint();
    }
}
