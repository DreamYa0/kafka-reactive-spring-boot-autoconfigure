package com.g7.framework.reactive.kafka;

import com.g7.framework.reactive.kafka.listener.ShutdownHookListener;
import com.g7.framework.reactive.kafka.producer.ReactiveKafkaTemplate;
import com.g7.framework.reactive.kafka.properties.KafkaProperties;
import com.g7.framework.reactive.kafka.util.ReadPropertiesUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StringUtils;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.internals.ProducerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * @author dreamyao
 * @title
 * @date 2018/8/5 上午11:39
 * @since 1.0.0
 */
@AutoConfiguration
@EnableConfigurationProperties(KafkaProperties.class)
public class ReactiveKafkaAutoConfiguration {

    private final KafkaProperties properties;
    private final ProducerFactory producerFactory;

    public ReactiveKafkaAutoConfiguration(KafkaProperties properties, ProducerFactory producerFactory) {
        this.properties = properties;
        this.producerFactory = producerFactory;
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaSender<String,String> kafkaSender() {
        Properties properties = buildProducerProperties();
        final SenderOptions<String, String> options = SenderOptions.<String,String>create(properties)
                .maxInFlight(1000)
                .closeTimeout(Duration.ofSeconds(30))
                .scheduler(Schedulers.boundedElastic());
        return KafkaSender.create(producerFactory, options);
    }

    @Bean
    @ConditionalOnMissingBean
    public ReactiveKafkaTemplate reactiveKafkaTemplate(@Autowired KafkaSender<String, String> kafkaSender) {
        return new ReactiveKafkaTemplate(kafkaSender);
    }

    @Bean
    @ConditionalOnMissingBean(value = ShutdownHookListener.class)
    public ShutdownHookListener shutdownHookListener(KafkaSender<String, String> kafkaSender) {
        return new ShutdownHookListener(kafkaSender);
    }

    private Properties buildProducerProperties() {

        Properties defaultProducerProperties = ReadPropertiesUtils.readProducerDefaultProperties();
        defaultProducerProperties.setProperty("bootstrap.servers", properties.getBootstrap().getServers());

        String keySerializer = properties.getProducer().getKeySerializer();
        if (StringUtils.hasText(keySerializer)) {
            defaultProducerProperties.setProperty("key.serializer", keySerializer);
        }

        String valueSerializer = properties.getProducer().getValueSerializer();
        if (StringUtils.hasText(valueSerializer)) {
            defaultProducerProperties.setProperty("value.serializer", valueSerializer);
        }
        return defaultProducerProperties;
    }
}
