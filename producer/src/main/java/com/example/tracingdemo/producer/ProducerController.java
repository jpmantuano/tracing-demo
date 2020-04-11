package com.example.tracingdemo.producer;

import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@RestController
public class ProducerController {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ProducerController.class);

    String topic = "jaeger-tracing";
    String bootstrapServer = "192.168.100.9:9092";

    private Tracer tracer;
    private TracingKafkaProducer<String, String> traceableProducer;
    private ScheduledExecutorService executor;

    @Autowired
    public ProducerController(Tracer tracer) {
        this.tracer = tracer;
    }

    @GetMapping("/stop")
    public String stop() {
        executor.shutdown();
        return "Stopped...";
    }

    @GetMapping("/start")
    public String start() {
        traceableProducer = createTraceableProducer(createKafkaProducer());

        Runnable runnable = () -> {
            String randomPayload = random();
            LOGGER.info("Sending a random message: {}", randomPayload);
            traceableProducer.send(new ProducerRecord<>(topic, randomPayload));
        };

        Random random = new Random();

        executor = Executors.newScheduledThreadPool(2);
        Runnable scheduleRunnable = () -> {
            while (true) {
                int randomSleep = random.nextInt(30 - 1 + 1) + 1;
                executor.schedule(runnable, randomSleep, TimeUnit.SECONDS);
            }
        };

        new Thread(scheduleRunnable).start();

        return "started...";
    }

    private String random() {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10;
        Random random = new Random();

        return random.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    private TracingKafkaProducer<String, String> createTraceableProducer(KafkaProducer<String, String> kafkaProducer) {
        try {
            return new TracingKafkaProducer<>(kafkaProducer, tracer);
        } catch (Exception e) {
            LOGGER.info("Exception encountered while creating a producer: {0}", e);
            return null;
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(properties);
    }
}