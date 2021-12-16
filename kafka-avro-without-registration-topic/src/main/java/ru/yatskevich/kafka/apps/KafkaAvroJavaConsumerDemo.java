package ru.yatskevich.kafka.apps;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class KafkaAvroJavaConsumerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();

        // normal producer
        List<String> servers = List.of("vm-kafka-acl01t.ds.strp.tinkoff.cloud:9093",
                "vm-kafka-acl02t.ds.strp.tinkoff.cloud:9093",
                "vm-kafka-acl03t.ds.strp.tinkoff.cloud:9093",
                "vm-kafka-acl04t.ds.strp.tinkoff.cloud:9093",
                "vm-kafka-acl05t.ds.strp.tinkoff.cloud:9093",
                "vm-kafka-acl06t.ds.strp.tinkoff.cloud:9093",
                "vm-kafka-acl07t.ds.strp.tinkoff.cloud:9093");
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, servers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "Consumer_v1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "prm.pay.avro_v1");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "SCRAM-SHA-512");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"prm.pay\" password=\"4i>4.B93%|+,\";");

        properties.put("schema.registry.url", "http://vm-kafka-aclt-sr01.ds.strp.tinkoff.cloud:8081");

        KafkaConsumer<String, GenericRecord> kafkaConsumer = new KafkaConsumer<>(properties);
        String topic = "prm.pay.doc.payment";

        kafkaConsumer.subscribe(Collections.singleton(topic));

        while (true) {
            try {
                System.out.println("Polling");
                ConsumerRecords<String, GenericRecord> records = kafkaConsumer.poll(1000);

                for (ConsumerRecord<String, GenericRecord> record : records) {
                    GenericRecord totalAmounts = record.value();
                    System.out.println(totalAmounts);
                }

                kafkaConsumer.commitSync();
            } catch (SerializationException se) {
                kafkaConsumer.commitSync();
            }

        }
    }
}
