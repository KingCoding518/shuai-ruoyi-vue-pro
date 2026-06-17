package cn.iocoder.yudao.framework.mq.kafka.core.message;

import cn.hutool.core.util.TypeUtil;
import cn.iocoder.yudao.framework.common.util.json.JsonUtils;
import cn.iocoder.yudao.framework.mq.kafka.core.KafkaMQTemplate;
import cn.iocoder.yudao.framework.mq.kafka.core.interceptor.KafkaMessageInterceptor;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Kafka 消息监听器抽象类，用于实现集群消费
 *
 * @param <T> 消息类型。一定要填写噢，不然会报错
 *
 * @author 芋道源码
 */
@Slf4j
public abstract class AbstractKafkaMessageListener<T extends AbstractKafkaMessage> {

    /**
     * 消息类型
     */
    private final Class<T> messageType;
    /**
     * Kafka Topic
     */
    @Getter
    private final String topic;

    /**
     * Kafka 消费者分组，默认使用 spring.application.name 名字
     */
    @Value("${spring.application.name}")
    @Getter
    private String group;

    /**
     * 消费并发数，默认 1
     * 可通过 spring.kafka.listener.concurrency 全局配置，
     * 也可在子类构造函数中覆盖此字段来针对单个监听器配置
     */
    @Value("${spring.kafka.listener.concurrency:1}")
    @Getter
    private Integer concurrency;

    /**
     * KafkaMQTemplate
     */
    @Setter
    private KafkaMQTemplate kafkaMQTemplate;

    @SneakyThrows
    protected AbstractKafkaMessageListener() {
        this.messageType = getMessageClass();
        this.topic = messageType.getDeclaredConstructor().newInstance().getTopic();
    }

    /**
     * 处理消息的具体方法
     *
     * @param message 消息
     */
    public abstract void onMessage(T message);

    /**
     * 内部消息处理入口，由自动配置创建的 Container 回调
     *
     * @param messageJson JSON 格式的消息
     */
    public void handleMessage(String messageJson) {
        T messageObj = JsonUtils.parseObject(messageJson, messageType);
        try {
            consumeMessageBefore(messageObj);
            // 消费消息
            this.onMessage(messageObj);
        } catch (Exception e) {
            log.error("[handleMessage][消息({}) 处理异常]", messageObj, e);
            throw e;
        } finally {
            consumeMessageAfter(messageObj);
        }
    }

    /**
     * 通过解析类上的泛型，获得消息类型
     *
     * @return 消息类型
     */
    @SuppressWarnings("unchecked")
    private Class<T> getMessageClass() {
        Type type = TypeUtil.getTypeArgument(getClass(), 0);
        if (type == null) {
            throw new IllegalStateException(String.format("类型(%s) 需要设置消息类型", getClass().getName()));
        }
        return (Class<T>) type;
    }

    private void consumeMessageBefore(AbstractKafkaMessage message) {
        if (kafkaMQTemplate == null) {
            return;
        }
        List<KafkaMessageInterceptor> interceptors = kafkaMQTemplate.getInterceptors();
        // 正序
        interceptors.forEach(interceptor -> interceptor.consumeMessageBefore(message));
    }

    private void consumeMessageAfter(AbstractKafkaMessage message) {
        if (kafkaMQTemplate == null) {
            return;
        }
        List<KafkaMessageInterceptor> interceptors = kafkaMQTemplate.getInterceptors();
        // 倒序
        for (int i = interceptors.size() - 1; i >= 0; i--) {
            interceptors.get(i).consumeMessageAfter(message);
        }
    }

}
