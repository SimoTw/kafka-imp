package simo.kafkaimp3;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class KafkaProducerWrapper {
    private Producer<String, String> producer;
    public KafkaProducerWrapper() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9094,localhost:9096");
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(props);
    }
    public void send() {
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("imp3", Integer.toString(i)), (metadata, exception) -> {
                System.out.println(metadata);
                System.out.println(exception);
            });
        }
        producer.flush();
    }
    public static void main(String[] args) {
        System.out.println("statred producer \n");
        KafkaProducerWrapper p = new KafkaProducerWrapper();
        p.send();
    }
}
