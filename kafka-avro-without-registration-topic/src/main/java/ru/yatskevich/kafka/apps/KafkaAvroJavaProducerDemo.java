package ru.yatskevich.kafka.apps;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.tinkoff.prm.model.uprm.ReportTotalAmounts;

import java.util.List;
import java.util.Properties;

public class KafkaAvroJavaProducerDemo {

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
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "10");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "ClientId");
        properties.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "5000");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "SCRAM-SHA-512");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"prm.doc\" password=\"2@71fC8<%_+%\";");

        properties.put("schema.registry.url", "http://vm-kafka-aclt-sr01.ds.strp.tinkoff.cloud:8081");

        Producer<String, ReportTotalAmounts> producer = new KafkaProducer<>(properties);
        String topic = "prm.pay.doc.payment";

        ReportTotalAmounts reportTotalAmounts = ReportTotalAmounts.newBuilder()
                .setTotalAmountWithVat(123.12f)
                .setTotalAmountVat(321.32f)
                .build();

        ProducerRecord<String, ReportTotalAmounts> producerRecord = new ProducerRecord<>(
                topic, null, reportTotalAmounts);

        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                System.out.println(metadata);
            } else {
                exception.printStackTrace();
            }
        });

        producer.flush();
        producer.close();
    }
}
