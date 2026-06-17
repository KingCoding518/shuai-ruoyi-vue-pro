package cn.iocoder.yudao.framework.mq.kafka.core;

import cn.iocoder.yudao.framework.mq.kafka.core.interceptor.KafkaMessageInterceptor;
import cn.iocoder.yudao.framework.mq.kafka.core.message.AbstractKafkaMessage;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka MQ 操作模板类
 *
 * @author 芋道源码
 */
@AllArgsConstructor
@Slf4j
public class KafkaMQTemplate {

    @Getter
    private final KafkaTemplate<String, Object> kafkaTemplate;
    /**
     * 拦截器数组
     */
    @Getter
    private final List<KafkaMessageInterceptor> interceptors = new ArrayList<>();

    // ==================== 使用消息类自带 Topic ====================

    /**
     * 同步发送 Kafka 消息（使用消息类自带的 Topic）
     *
     * @param message 消息
     * @return 发送结果
     */
    public <T extends AbstractKafkaMessage> SendResult<String, Object> send(T message) {
        return send(message.getTopic(), message, 10, TimeUnit.SECONDS);
    }

    /**
     * 同步发送 Kafka 消息（使用消息类自带的 Topic），指定超时时间
     *
     * @param message 消息
     * @param timeout 超时时间
     * @param unit    时间单位
     * @return 发送结果
     */
    public <T extends AbstractKafkaMessage> SendResult<String, Object> send(T message, long timeout, TimeUnit unit) {
        return send(message.getTopic(), message, timeout, unit);
    }

    /**
     * 异步发送 Kafka 消息（使用消息类自带的 Topic）
     *
     * @param message 消息
     */
    public <T extends AbstractKafkaMessage> void sendAsync(T message) {
        sendAsync(message.getTopic(), message);
    }

    // ==================== 自定义 Topic 发送 ====================

    /**
     * 同步发送 Kafka 消息到指定 Topic
     *
     * @param topic   自定义 Topic
     * @param message 消息
     * @return 发送结果
     */
    public <T extends AbstractKafkaMessage> SendResult<String, Object> send(String topic, T message) {
        return send(topic, message, 10, TimeUnit.SECONDS);
    }

    /**
     * 同步发送 Kafka 消息到指定 Topic，指定超时时间
     *
     * @param topic   自定义 Topic
     * @param message 消息
     * @param timeout 超时时间
     * @param unit    时间单位
     * @return 发送结果
     */
    public <T extends AbstractKafkaMessage> SendResult<String, Object> send(String topic, T message, long timeout, TimeUnit unit) {
        try {
            sendMessageBefore(message);
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, message);
            return future.get(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(String.format("[send][topic(%s) 发送消息(%s) 被中断]", topic, message), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException(String.format("[send][topic(%s) 发送消息(%s) 失败]", topic, message), e);
        } finally {
            sendMessageAfter(message);
        }
    }

    /**
     * 异步发送 Kafka 消息到指定 Topic
     *
     * @param topic   自定义 Topic
     * @param message 消息
     */
    public <T extends AbstractKafkaMessage> void sendAsync(String topic, T message) {
        try {
            sendMessageBefore(message);
            kafkaTemplate.send(topic, message)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("[sendAsync][topic({}) 发送消息({}) 失败]", topic, message, ex);
                        }
                    });
        } finally {
            sendMessageAfter(message);
        }
    }

    /**
     * 添加拦截器
     *
     * @param interceptor 拦截器
     */
    public void addInterceptor(KafkaMessageInterceptor interceptor) {
        interceptors.add(interceptor);
    }

    private void sendMessageBefore(AbstractKafkaMessage message) {
        // 正序
        interceptors.forEach(interceptor -> interceptor.sendMessageBefore(message));
    }

    private void sendMessageAfter(AbstractKafkaMessage message) {
        // 倒序
        for (int i = interceptors.size() - 1; i >= 0; i--) {
            interceptors.get(i).sendMessageAfter(message);
        }
    }

}
