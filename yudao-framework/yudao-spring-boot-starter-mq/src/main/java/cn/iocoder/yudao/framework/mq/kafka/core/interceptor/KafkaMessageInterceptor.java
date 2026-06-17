package cn.iocoder.yudao.framework.mq.kafka.core.interceptor;

import cn.iocoder.yudao.framework.mq.kafka.core.message.AbstractKafkaMessage;

/**
 * {@link AbstractKafkaMessage} 消息拦截器
 * 通过拦截器，作为插件机制，实现拓展。
 * 例如说，多租户场景下的 MQ 消息处理
 *
 * @author 芋道源码
 */
public interface KafkaMessageInterceptor {

    default void sendMessageBefore(AbstractKafkaMessage message) {
    }

    default void sendMessageAfter(AbstractKafkaMessage message) {
    }

    default void consumeMessageBefore(AbstractKafkaMessage message) {
    }

    default void consumeMessageAfter(AbstractKafkaMessage message) {
    }

}
