package cn.iocoder.yudao.framework.mq.kafka.core;

import cn.iocoder.yudao.framework.mq.constants.RabbitMqConstants;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Kafka MQ 操作模板类
 *
 * @author 芋道源码
 */
@AllArgsConstructor
@Slf4j
public class KafkaMqHelper {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ThreadPoolTaskExecutor executor;

    public KafkaMqHelper(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        // 初始化异步发送线程池
        executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(15);
        executor.setQueueCapacity(99999);
        executor.setThreadNamePrefix("mq-async-send-handler");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
    }

    /**
     * 同步发送消息到指定 topic（阻塞等待发送结果）
     *
     * @param topic 主题
     * @param data  消息数据
     * @param <T>   数据类型
     */
    public <T> void send(String topic, T data) {
        send(topic, null, data);
    }

    /**
     * 同步发送消息到指定 topic，并指定 key（阻塞等待发送结果）
     *
     * @param topic 主题
     * @param key   消息 key（可用于分区路由）
     * @param data  消息数据
     * @param <T>   数据类型
     */
    public <T> void send(String topic, String key, T data) {
        log.debug("准备发送消息，topic：{}， key：{}， message：{}", topic, key, data);
        try {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, data);
            // 阻塞等待发送结果，确保发送完成
            future.get();
        } catch (InterruptedException e) {
            log.error("发送消息被中断，topic:{}， data:{}", topic, data, e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.error("发送消息失败，topic:{}， data:{}", topic, data, e);
            throw new RuntimeException("Kafka 消息发送失败", e);
        }
    }

    // ==================== 异步发送 ====================

    /**
     * 异步发送消息到指定 topic，支持延迟发送
     *
     * @param topic   主题
     * @param data    消息数据
     * @param delayMs 延迟毫秒数（为 null 或 <= 0 时不延迟）
     * @param <T>     数据类型
     */
    public <T> void sendAsync(String topic, T data, Long delayMs) {
        String requestId = MDC.get(RabbitMqConstants.REQUEST_ID_HEADER);
        CompletableFuture.runAsync(() -> {
            try {
                MDC.put(RabbitMqConstants.REQUEST_ID_HEADER, requestId);
                if (delayMs != null && delayMs > 0) {
                    Thread.sleep(delayMs);
                }
                kafkaTemplate.send(topic, data).whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("异步发送消息异常，topic:{}， data:{}", topic, data, ex);
                    } else {
                        log.debug("异步发送消息成功，topic:{}， offset:{}", topic, result.getRecordMetadata().offset());
                    }
                });
            } catch (Exception e) {
                log.error("异步发送消息异常，topic:{}， data:{}", topic, data, e);
            }
        }, executor);
    }

    /**
     * 异步发送消息到指定 topic，支持延迟发送和指定 key
     *
     * @param topic   主题
     * @param key     消息 key
     * @param data    消息数据
     * @param delayMs 延迟毫秒数
     * @param <T>     数据类型
     */
    public <T> void sendAsync(String topic, String key, T data, Long delayMs) {
        String requestId = MDC.get(RabbitMqConstants.REQUEST_ID_HEADER);
        CompletableFuture.runAsync(() -> {
            try {
                MDC.put(RabbitMqConstants.REQUEST_ID_HEADER, requestId);
                if (delayMs != null && delayMs > 0) {
                    Thread.sleep(delayMs);
                }
                kafkaTemplate.send(topic, key, data).whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("异步发送消息异常，topic:{}， key:{}， data:{}", topic, key, data, ex);
                    } else {
                        log.debug("异步发送消息成功，topic:{}， key:{}， offset:{}", topic, key, result.getRecordMetadata().offset());
                    }
                });
            } catch (Exception e) {
                log.error("异步发送消息异常，topic:{}， key:{}， data:{}", topic, key, data, e);
            }
        }, executor);
    }

    /**
     * 异步发送消息到指定 topic（无延迟）
     *
     * @param topic 主题
     * @param data  消息数据
     * @param <T>   数据类型
     */
    public <T> void sendAsync(String topic, T data) {
        sendAsync(topic, data, null);
    }

    /**
     * 异步发送消息到指定 topic，并指定 key（无延迟）
     *
     * @param topic 主题
     * @param key   消息 key
     * @param data  消息数据
     * @param <T>   数据类型
     */
    public <T> void sendAsync(String topic, String key, T data) {
        sendAsync(topic, key, data, null);
    }

}
