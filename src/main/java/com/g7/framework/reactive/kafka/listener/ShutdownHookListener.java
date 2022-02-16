package com.g7.framework.reactive.kafka.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import reactor.kafka.sender.KafkaSender;

/**
 * @author dreamyao
 * @title
 * @date 2018/12/23 2:12 PM
 * @since 1.0.0
 */
public class ShutdownHookListener implements ApplicationListener<ContextClosedEvent> {

    private static final Logger logger = LoggerFactory.getLogger(ShutdownHookListener.class);
    private final KafkaSender<String, String> kafkaSender;

    public ShutdownHookListener(KafkaSender<String, String> kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        kafkaSender.close();
        logger.debug("kafka sender close success.");
    }
}
