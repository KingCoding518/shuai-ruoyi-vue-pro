package cn.iocoder.yudao.framework.mq.kafka.core.message;

import cn.iocoder.yudao.framework.mq.kafka.core.KafkaMQTemplate;
import cn.iocoder.yudao.framework.mq.kafka.core.interceptor.KafkaMessageInterceptor;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link AbstractKafkaMessageListener} 单元测试
 *
 * @author 芋道源码
 */
@ExtendWith(MockitoExtension.class)
public class AbstractKafkaMessageListenerTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    // ==================== Topic 自动发现 ====================

    /**
     * 测试：通过泛型自动发现 Topic（默认类名）
     */
    @Test
    public void testTopicDiscoveryFromClassName() {
        OrderMessageListener listener = new OrderMessageListener();
        assertEquals("OrderMessage", listener.getTopic());
    }

    /**
     * 测试：自定义 Topic 的消息类
     */
    @Test
    public void testCustomTopicMessage() {
        CustomTopicListener listener = new CustomTopicListener();
        assertEquals("payment-topic", listener.getTopic());
    }

    // ==================== handleMessage ====================

    /**
     * 测试：handleMessage 反序列化并回调 onMessage
     */
    @Test
    public void testHandleMessageJson() {
        OrderMessageListener listener = new OrderMessageListener();

        String json = "{\"orderId\":100,\"status\":\"PAID\"}";
        listener.handleMessage(json);

        assertNotNull(listener.receivedMessage);
        assertEquals(100L, listener.receivedMessage.getOrderId());
        assertEquals("PAID", listener.receivedMessage.getStatus());
    }

    /**
     * 测试：handleMessage 反序列化异常时向上抛出
     */
    @Test
    public void testHandleMessageInvalidJson() {
        OrderMessageListener listener = new OrderMessageListener();

        assertThrows(Exception.class, () -> listener.handleMessage("{invalid-json"));
    }

    // ==================== 拦截器调用 ====================

    /**
     * 测试：消费消息时拦截器被正确调用
     */
    @Test
    public void testInterceptorCalledOnHandleMessage() {
        OrderMessageListener listener = new OrderMessageListener();

        // 准备 KafkaMQTemplate
        KafkaMQTemplate kafkaMQTemplate = new KafkaMQTemplate(kafkaTemplate);
        AtomicInteger beforeCount = new AtomicInteger(0);
        AtomicInteger afterCount = new AtomicInteger(0);

        kafkaMQTemplate.addInterceptor(new KafkaMessageInterceptor() {
            @Override
            public void consumeMessageBefore(AbstractKafkaMessage message) {
                beforeCount.incrementAndGet();
            }

            @Override
            public void consumeMessageAfter(AbstractKafkaMessage message) {
                afterCount.incrementAndGet();
            }
        });
        listener.setKafkaMQTemplate(kafkaMQTemplate);

        // 执行
        listener.handleMessage("{\"orderId\":1,\"status\":\"CREATED\"}");

        assertEquals(1, beforeCount.get());
        assertEquals(1, afterCount.get());
    }

    /**
     * 测试：未设置 KafkaMQTemplate 时，拦截器不触发但不报错
     */
    @Test
    public void testNoKafkaMQTemplateDoesNotFail() {
        OrderMessageListener listener = new OrderMessageListener();
        // 不设置 kafkaMQTemplate

        assertDoesNotThrow(() ->
                listener.handleMessage("{\"orderId\":1,\"status\":\"OK\"}"));
    }

    // ==================== 泛型缺失时的异常 ====================

    /**
     * 测试：未指定泛型类型时抛异常
     */
    @Test
    public void testMissingGenericThrowsException() {
        assertThrows(IllegalStateException.class, NoGenericListener::new);
    }

    // ==================== 测试用的内部类 ====================

    /**
     * 订单消息
     */
    @Data
    @EqualsAndHashCode(callSuper = true)
    static class OrderMessage extends AbstractKafkaMessage {
        private Long orderId;
        private String status;
    }

    /**
     * 订单消息监听器
     */
    static class OrderMessageListener extends AbstractKafkaMessageListener<OrderMessage> {
        OrderMessage receivedMessage;

        @Override
        public void onMessage(OrderMessage message) {
            this.receivedMessage = message;
        }
    }

    /**
     * 自定义 Topic 的消息
     */
    @Data
    @EqualsAndHashCode(callSuper = true)
    static class PaymentMessage extends AbstractKafkaMessage {

        @Override
        @JsonIgnore
        public String getTopic() {
            return "payment-topic";
        }
    }

    /**
     * 自定义 Topic 的监听器
     */
    static class CustomTopicListener extends AbstractKafkaMessageListener<PaymentMessage> {

        @Override
        public void onMessage(PaymentMessage message) {
        }
    }

    /**
     * 未指定泛型的监听器（应抛异常）
     */
    static class NoGenericListener extends AbstractKafkaMessageListener {
        @Override
        public void onMessage(AbstractKafkaMessage message) {
        }
    }

}
