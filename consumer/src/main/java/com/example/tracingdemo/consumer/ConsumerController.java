package com.example.tracingdemo.consumer;

import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaConsumer;
import io.opentracing.contrib.kafka.TracingKafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@RestController
public class ConsumerController {
    String topic = "jaeger-tracing";
    String bootstrapServer = "192.168.100.9:9092";

    private Tracer tracer;

    @Autowired
    public ConsumerController(Tracer tracer) {
        this.tracer = tracer;
    }

    @GetMapping("/consume")
    public void run() {
        try (KafkaConsumer<String, String> consumer = startConsumer()) {
            final TracingKafkaConsumer<String, String> tracingKafkaConsumer = new TracingKafkaConsumer<String, String>(consumer, tracer);
            tracingKafkaConsumer.subscribe(Collections.singletonList(topic));

            ConsumerRecords<String, String> consumerRecords = tracingKafkaConsumer.poll(Duration.ofSeconds(30));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                final SpanContext context =
                        TracingKafkaUtils.extractSpanContext(consumerRecord.headers(), tracer);
                tracer.buildSpan("consumption")
                        .withTag("user", consumerRecord.key())
                        .asChildOf(context)
                        .start();
                try {
                    System.out.println(consumerRecord.value());
                    tracingKafkaConsumer.commitSync();
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private KafkaConsumer<String, String> startConsumer() {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(properties);
    }
}
