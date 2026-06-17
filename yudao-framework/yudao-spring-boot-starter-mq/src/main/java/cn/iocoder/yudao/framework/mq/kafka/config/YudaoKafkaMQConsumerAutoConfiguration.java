package cn.iocoder.yudao.framework.mq.kafka.config;

import cn.iocoder.yudao.framework.mq.kafka.core.KafkaMQTemplate;
import cn.iocoder.yudao.framework.mq.kafka.core.message.AbstractKafkaMessageListener;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka 消息队列 Consumer 配置类
 * <p>
 * 动态扫描所有 {@link AbstractKafkaMessageListener} Bean，
 * 复用 Spring Boot 自动配置的 {@link ConcurrentKafkaListenerContainerFactory}，
 * 为每个监听器创建对应的 {@link ConcurrentMessageListenerContainer} 进行消费。
 *
 * <p>相比自己 new ConsumerFactory，这样做的优势：
 * <ul>
 *   <li>✅ 继承用户在 YAML 中配置的序列化器（不强制覆盖）</li>
 *   <li>✅ 继承 spring.kafka.consumer 的所有配置（bootstrap-servers、SSL、SASL 等）</li>
 *   <li>✅ 继承 spring.kafka.listener 的 ack-mode、concurrency 等</li>
 *   <li>✅ 继承用户自定义的 CommonErrorHandler（死信、重试）</li>
 *   <li>✅ 每个 Listener 可独立设置 groupId 和 concurrency</li>
 * </ul>
 *
 * @author 芋道源码
 */
@Slf4j
@AutoConfiguration
@ConditionalOnClass(KafkaTemplate.class)
@ConditionalOnBean(AbstractKafkaMessageListener.class)
public class YudaoKafkaMQConsumerAutoConfiguration {

    private final List<ConcurrentMessageListenerContainer<String, String>> containers = new ArrayList<>();

    @Bean
    public KafkaListenerContainerRegistry kafkaListenerContainerRegistry(
            KafkaMQTemplate kafkaMQTemplate,
            ConcurrentKafkaListenerContainerFactory<String, String> factory,
            List<AbstractKafkaMessageListener<?>> listeners) {

        // 从 Spring Boot 自动配置的工厂中取 ConsumerFactory（继承了用户所有配置）
        ConsumerFactory<? super String, ? super String> baseConsumerFactory = factory.getConsumerFactory();

        for (AbstractKafkaMessageListener<?> listener : listeners) {
            // 设置 kafkaMQTemplate，用于拦截器访问
            listener.setKafkaMQTemplate(kafkaMQTemplate);

            // 1. 基于工厂的 ConsumerFactory 创建每个 listener 专属的 ConsumerFactory
            //    只覆盖 groupId，其余配置（序列化器、bootstrap-servers等）全部继承
            Map<String, Object> consumerProps = new HashMap<>(
                    baseConsumerFactory.getConfigurationProperties());
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, listener.getGroup());
            DefaultKafkaConsumerFactory<String, String> listenerConsumerFactory =
                    new DefaultKafkaConsumerFactory<>(consumerProps);

            // 2. 创建 ContainerProperties
            ContainerProperties containerProperties = new ContainerProperties(listener.getTopic());
            containerProperties.setAckMode(factory.getContainerProperties().getAckMode());
            containerProperties.setMissingTopicsFatal(false);
            containerProperties.setMessageListener((MessageListener<String, String>) message -> {
                listener.handleMessage(message.value());
            });

            // 3. 创建 ConcurrentMessageListenerContainer
            ConcurrentMessageListenerContainer<String, String> container =
                    new ConcurrentMessageListenerContainer<>(listenerConsumerFactory, containerProperties);
            container.setConcurrency(listener.getConcurrency());
            container.start();
            containers.add(container);

            log.info("[kafkaListenerContainerRegistry][注册 Topic({}) Group({}) concurrency({}) 对应的监听器({})]",
                    listener.getTopic(), listener.getGroup(), listener.getConcurrency(),
                    listener.getClass().getName());
        }

        return new KafkaListenerContainerRegistry(containers);
    }

    @PreDestroy
    public void destroy() {
        for (ConcurrentMessageListenerContainer<String, String> container : containers) {
            try {
                container.stop();
                log.info("[destroy][关闭 Kafka 消费者容器成功]");
            } catch (Exception e) {
                log.error("[destroy][关闭 Kafka 消费者容器异常]", e);
            }
        }
    }

    /**
     * Kafka 监听器容器注册表，用于统一管理所有容器的生命周期
     */
    public record KafkaListenerContainerRegistry(
            List<ConcurrentMessageListenerContainer<String, String>> containers) {
    }

}
