package com.g7.framework.reactive.kafka.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author dreamyao
 * @title
 * @date 2018/6/15 下午10:04
 * @since 1.0.0
 */
public interface DefaultMessageComsumer<K, V> extends GenericMessageComsumer<ConsumerRecord<K, V>> {

}
  