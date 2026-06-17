package cn.iocoder.yudao.framework.mq.kafka.core.message;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 消息抽象基类
 *
 * @author 芋道源码
 */
@Data
public abstract class AbstractKafkaMessage {

    /**
     * 头
     */
    private Map<String, String> headers = new HashMap<>();

    /**
     * 获得 Kafka Topic，默认使用类名
     *
     * @return Topic
     */
    @JsonIgnore // 避免序列化。原因是，Kafka 发送消息的时候，已经会指定 Topic
    public String getTopic() {
        return getClass().getSimpleName();
    }

    public String getHeader(String key) {
        return headers.get(key);
    }

    public void addHeader(String key, String value) {
        headers.put(key, value);
    }

}
