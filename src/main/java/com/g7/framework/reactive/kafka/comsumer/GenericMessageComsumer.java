package com.g7.framework.reactive.kafka.comsumer;

import reactor.core.publisher.Mono;

/**
 * 消息监听抽象接口
 * @author dreamyao
 * @version V1.0
 * @date 2018/6/15 下午10:04
 */
public interface GenericMessageComsumer<T> extends Comsumer<T> {
    Mono<Object> record(T record);
}
  