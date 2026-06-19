package cn.iocoder.yudao.framework.mq.kafka.config;

import cn.iocoder.yudao.framework.mq.kafka.core.KafkaMqHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

/**
 * Kafka @KafkaListener 注解消费模式配置
 *
 * @author 芋道源码
 */
@Slf4j
@Configuration
/**
 * 当 classpath 中存在 KafkaTemplate 这个类时，才加载这个配置类
 * 场景是引入多个MQ的时候，使用的不是kafka就直接跳过配置
 */
@ConditionalOnClass(KafkaTemplate.class)
public class YudaoKafkaMQAutoConfiguration {

    /**
     * kafka发送工具
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(KafkaTemplate.class)
    public KafkaMqHelper aafkaMqHelper(KafkaTemplate<String, Object> kafkaTemplate) {
        return new KafkaMqHelper(kafkaTemplate);
    }

    /**
     * 批量消费监听器容器工厂
     * 解决 List<T> 参数在批量消费模式下的类型解析问题
     */
    @Bean
    @ConditionalOnMissingBean(name = "kafkaBatchContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaBatchContainerFactory(
            KafkaProperties kafkaProperties) {
        // 1. 从 YAML 配置构建 Consumer 属性，复用已有的 JsonDeserializer、trusted.packages 等配置
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);

        // 2. 显式确保 JsonDeserializer 通过 __TypeId__ Header 反序列化
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);

        // 3. 构建 ConsumerFactory
        DefaultKafkaConsumerFactory<String, Object> consumerFactory =
                new DefaultKafkaConsumerFactory<>(props);

        // 4. 创建并配置批量监听器容器工厂
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true); // 开启批量消费模式
        factory.getContainerProperties().setMissingTopicsFatal(false);

        log.info("[kafkaBatchContainerFactory][创建批量消费监听器容器工厂成功]");
        return factory;
    }

}
