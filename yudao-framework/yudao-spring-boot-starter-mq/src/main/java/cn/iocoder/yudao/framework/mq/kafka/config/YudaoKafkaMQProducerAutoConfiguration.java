package cn.iocoder.yudao.framework.mq.kafka.config;

import cn.iocoder.yudao.framework.mq.kafka.core.KafkaMQTemplate;
import cn.iocoder.yudao.framework.mq.kafka.core.interceptor.KafkaMessageInterceptor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

/**
 * Kafka 消息队列 Producer 配置类
 *
 * @author 芋道源码
 */
@Slf4j
@AutoConfiguration
@ConditionalOnClass(KafkaTemplate.class)
public class YudaoKafkaMQProducerAutoConfiguration {

    @Bean
    public KafkaMQTemplate kafkaMQTemplate(KafkaTemplate<String, Object> kafkaTemplate,
                                           List<KafkaMessageInterceptor> interceptors) {
        KafkaMQTemplate kafkaMQTemplate = new KafkaMQTemplate(kafkaTemplate);
        // 添加拦截器
        interceptors.forEach(kafkaMQTemplate::addInterceptor);
        log.info("[kafkaMQTemplate][创建 KafkaMQTemplate 成功]");
        return kafkaMQTemplate;
    }

}
