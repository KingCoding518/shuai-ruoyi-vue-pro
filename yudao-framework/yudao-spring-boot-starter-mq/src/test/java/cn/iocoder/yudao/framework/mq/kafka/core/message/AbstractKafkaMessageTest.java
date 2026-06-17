package cn.iocoder.yudao.framework.mq.kafka.core.message;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link AbstractKafkaMessage} 单元测试
 *
 * @author 芋道源码
 */
public class AbstractKafkaMessageTest {

    /**
     * 测试：默认 Topic 为类名
     */
    @Test
    public void testDefaultTopic() {
        OrderMessage message = new OrderMessage();
        assertEquals("OrderMessage", message.getTopic());
    }

    /**
     * 测试：自定义 Topic
     */
    @Test
    public void testCustomTopic() {
        CustomTopicMessage message = new CustomTopicMessage();
        assertEquals("my-custom-topic", message.getTopic());
    }

    /**
     * 测试：headers 的存取
     */
    @Test
    public void testHeaders() {
        OrderMessage message = new OrderMessage();
        message.addHeader("traceId", "abc123");
        message.addHeader("tenantId", "1");

        assertEquals("abc123", message.getHeader("traceId"));
        assertEquals("1", message.getHeader("tenantId"));
        assertNull(message.getHeader("nonexistent"));
    }

    /**
     * 测试：headers 初始为空
     */
    @Test
    public void testHeadersInitiallyEmpty() {
        OrderMessage message = new OrderMessage();
        assertNotNull(message.getHeaders());
        assertTrue(message.getHeaders().isEmpty());
    }

    // ==================== 测试用的内部类 ====================

    @Data
    @EqualsAndHashCode(callSuper = true)
    static class OrderMessage extends AbstractKafkaMessage {
        private Long orderId;
        private String status;
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    static class CustomTopicMessage extends AbstractKafkaMessage {

        @Override
        @JsonIgnore
        public String getTopic() {
            return "my-custom-topic";
        }
    }

}
