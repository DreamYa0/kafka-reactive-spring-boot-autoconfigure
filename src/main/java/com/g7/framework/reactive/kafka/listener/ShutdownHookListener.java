package com.g7.framework.reactive.kafka.listener;

import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import reactor.kafka.sender.KafkaSender;

/**
 * @author dreamyao
 * @title
 * @date 2018/12/23 2:12 PM
 * @since 1.0.0
 */
public class ShutdownHookListener<K,V> implements ApplicationListener<ContextClosedEvent> {

    private final KafkaSender<K, V> kafkaSender;

    public ShutdownHookListener(KafkaSender<K, V> kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        kafkaSender.close();
    }
}
