package com.example.tracingdemo.producer;

import io.jaegertracing.internal.samplers.ConstSampler;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
public class TracerConfig {

    @Value("${jaeger.tracer.host:localhost}")
    private String jaegerHost;
    @Value("${jaeger.tracer.port:6831}")
    private Integer jaegerPort;
    @Value("${spring.application.name:demo-producer}")
    private String applicationName;

    @Bean
    public Tracer tracer() {
        return io.jaegertracing.Configuration.fromEnv(applicationName)
                .withSampler(
                        io.jaegertracing.Configuration.SamplerConfiguration.fromEnv()
                                .withType(ConstSampler.TYPE)
                                .withParam(1))
                .withReporter(
                        io.jaegertracing.Configuration.ReporterConfiguration.fromEnv()
                                .withLogSpans(true)
                                .withFlushInterval(1000)
                                .withMaxQueueSize(10000)
                                .withSender(
                                        io.jaegertracing.Configuration.SenderConfiguration.fromEnv()
                                                .withAgentHost(jaegerHost)
                                                .withAgentPort(jaegerPort)
                                ))
                .getTracer();
    }

    @PostConstruct
    public void registerToGlobalTracer() {
        GlobalTracer.registerIfAbsent(this::tracer);
    }
}
