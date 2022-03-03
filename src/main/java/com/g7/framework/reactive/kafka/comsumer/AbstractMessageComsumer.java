package com.g7.framework.reactive.kafka.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Mono;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public interface AbstractMessageComsumer extends Comsumer<ConsumerRecord<String, String>> {

    Mono<?> consume(ConsumerRecord<String, String> record);
}
  