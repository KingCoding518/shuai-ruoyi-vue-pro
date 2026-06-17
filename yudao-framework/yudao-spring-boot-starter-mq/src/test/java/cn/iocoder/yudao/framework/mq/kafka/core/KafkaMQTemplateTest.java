package cn.iocoder.yudao.framework.mq.kafka.core;

import cn.iocoder.yudao.framework.mq.kafka.core.interceptor.KafkaMessageInterceptor;
import cn.iocoder.yudao.framework.mq.kafka.core.message.AbstractKafkaMessage;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * {@link KafkaMQTemplate} 单元测试
 *
 * @author 芋道源码
 */
@ExtendWith(MockitoExtension.class)
public class KafkaMQTemplateTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private KafkaMQTemplate kafkaMQTemplate;

    @BeforeEach
    public void setUp() {
        kafkaMQTemplate = new KafkaMQTemplate(kafkaTemplate);
    }

    // ==================== send() 使用消息自带 Topic ====================

    /**
     * 测试：同步发送，使用消息类默认 Topic
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSendWithDefaultTopic() throws Exception {
        // 准备
        OrderMessage message = new OrderMessage();
        message.setOrderId(1L);
        message.setStatus("CREATED");

        CompletableFuture<SendResult<String, Object>> future = mock(CompletableFuture.class);
        SendResult<String, Object> sendResult = mock(SendResult.class);
        when(future.get(10, TimeUnit.SECONDS)).thenReturn(sendResult);
        when(kafkaTemplate.send(eq("OrderMessage"), eq(message))).thenReturn(future);

        // 执行
        SendResult<String, Object> result = kafkaMQTemplate.send(message);

        // 断言
        assertSame(sendResult, result);
        verify(kafkaTemplate).send(eq("OrderMessage"), eq(message));
    }

    /**
     * 测试：同步发送（消息自带 Topic），自定义超时
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSendWithDefaultTopicAndCustomTimeout() throws Exception {
        OrderMessage message = new OrderMessage();
        message.setOrderId(1L);

        CompletableFuture<SendResult<String, Object>> future = mock(CompletableFuture.class);
        SendResult<String, Object> sendResult = mock(SendResult.class);
        when(future.get(5, TimeUnit.SECONDS)).thenReturn(sendResult);
        when(kafkaTemplate.send(eq("OrderMessage"), eq(message))).thenReturn(future);

        SendResult<String, Object> result = kafkaMQTemplate.send(message, 5, TimeUnit.SECONDS);

        assertSame(sendResult, result);
    }

    // ==================== send(String topic, T message) 自定义 Topic ====================

    /**
     * 测试：同步发送到自定义 Topic
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSendWithCustomTopic() throws Exception {
        OrderMessage message = new OrderMessage();
        message.setOrderId(1L);

        CompletableFuture<SendResult<String, Object>> future = mock(CompletableFuture.class);
        SendResult<String, Object> sendResult = mock(SendResult.class);
        when(future.get(10, TimeUnit.SECONDS)).thenReturn(sendResult);
        when(kafkaTemplate.send(eq("custom-order-topic"), eq(message))).thenReturn(future);

        SendResult<String, Object> result = kafkaMQTemplate.send("custom-order-topic", message);

        assertSame(sendResult, result);
        verify(kafkaTemplate).send(eq("custom-order-topic"), eq(message));
    }

    /**
     * 测试：同步发送到自定义 Topic + 自定义超时
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSendWithCustomTopicAndTimeout() throws Exception {
        OrderMessage message = new OrderMessage();
        message.setOrderId(1L);

        CompletableFuture<SendResult<String, Object>> future = mock(CompletableFuture.class);
        SendResult<String, Object> sendResult = mock(SendResult.class);
        when(future.get(3, TimeUnit.SECONDS)).thenReturn(sendResult);
        when(kafkaTemplate.send(eq("my-topic"), eq(message))).thenReturn(future);

        SendResult<String, Object> result = kafkaMQTemplate.send("my-topic", message, 3, TimeUnit.SECONDS);

        assertSame(sendResult, result);
        verify(kafkaTemplate).send(eq("my-topic"), eq(message));
    }

    // ==================== sendAsync() 异步发送 ====================

    /**
     * 测试：异步发送，使用消息默认 Topic
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSendAsyncWithDefaultTopic() {
        OrderMessage message = new OrderMessage();
        message.setOrderId(1L);

        CompletableFuture<SendResult<String, Object>> future = mock(CompletableFuture.class);
        when(kafkaTemplate.send(eq("OrderMessage"), eq(message))).thenReturn(future);

        assertDoesNotThrow(() -> kafkaMQTemplate.sendAsync(message));

        verify(kafkaTemplate).send(eq("OrderMessage"), eq(message));
    }

    /**
     * 测试：异步发送到自定义 Topic
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSendAsyncWithCustomTopic() {
        OrderMessage message = new OrderMessage();
        message.setOrderId(1L);

        CompletableFuture<SendResult<String, Object>> future = mock(CompletableFuture.class);
        when(kafkaTemplate.send(eq("async-custom-topic"), eq(message))).thenReturn(future);

        assertDoesNotThrow(() -> kafkaMQTemplate.sendAsync("async-custom-topic", message));

        verify(kafkaTemplate).send(eq("async-custom-topic"), eq(message));
    }

    // ==================== 异常处理 ====================

    /**
     * 测试：发送被中断时抛异常
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSendInterrupted() throws Exception {
        OrderMessage message = new OrderMessage();
        CompletableFuture<SendResult<String, Object>> future = mock(CompletableFuture.class);
        when(future.get(10, TimeUnit.SECONDS)).thenThrow(new InterruptedException());
        when(kafkaTemplate.send(any(String.class), any())).thenReturn(future);

        assertThrows(IllegalStateException.class, () -> kafkaMQTemplate.send(message));
        assertTrue(Thread.interrupted()); // 验证中断状态已恢复
    }

    /**
     * 测试：发送超时时抛异常
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSendTimeout() throws Exception {
        OrderMessage message = new OrderMessage();
        CompletableFuture<SendResult<String, Object>> future = mock(CompletableFuture.class);
        when(future.get(2, TimeUnit.SECONDS)).thenThrow(new TimeoutException());
        when(kafkaTemplate.send(any(String.class), any())).thenReturn(future);

        assertThrows(IllegalStateException.class, () -> kafkaMQTemplate.send(message, 2, TimeUnit.SECONDS));
    }

    // ==================== 拦截器 ====================

    /**
     * 测试：拦截器在发送前后被调用
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testInterceptorCalled() throws Exception {
        // 准备拦截器
        AtomicInteger beforeCount = new AtomicInteger(0);
        AtomicInteger afterCount = new AtomicInteger(0);

        KafkaMessageInterceptor interceptor = new KafkaMessageInterceptor() {
            @Override
            public void sendMessageBefore(AbstractKafkaMessage message) {
                beforeCount.incrementAndGet();
                assertEquals("OrderMessage", message.getTopic());
            }

            @Override
            public void sendMessageAfter(AbstractKafkaMessage message) {
                afterCount.incrementAndGet();
            }
        };
        kafkaMQTemplate.addInterceptor(interceptor);

        // 准备消息和 mock
        OrderMessage message = new OrderMessage();
        CompletableFuture<SendResult<String, Object>> future = mock(CompletableFuture.class);
        SendResult<String, Object> sendResult = mock(SendResult.class);
        when(future.get(10, TimeUnit.SECONDS)).thenReturn(sendResult);
        when(kafkaTemplate.send(any(String.class), any())).thenReturn(future);

        // 执行
        kafkaMQTemplate.send(message);

        // 断言
        assertEquals(1, beforeCount.get(), "sendMessageBefore 应被调用1次");
        assertEquals(1, afterCount.get(), "sendMessageAfter 应被调用1次");
    }

    /**
     * 测试：多个拦截器按顺序执行
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testMultipleInterceptorsOrder() throws Exception {
        StringBuilder trace = new StringBuilder();

        KafkaMessageInterceptor interceptor1 = new KafkaMessageInterceptor() {
            @Override
            public void sendMessageBefore(AbstractKafkaMessage message) {
                trace.append("B1->");
            }

            @Override
            public void sendMessageAfter(AbstractKafkaMessage message) {
                trace.append("A1->");
            }
        };
        KafkaMessageInterceptor interceptor2 = new KafkaMessageInterceptor() {
            @Override
            public void sendMessageBefore(AbstractKafkaMessage message) {
                trace.append("B2->");
            }

            @Override
            public void sendMessageAfter(AbstractKafkaMessage message) {
                trace.append("A2->");
            }
        };
        kafkaMQTemplate.addInterceptor(interceptor1);
        kafkaMQTemplate.addInterceptor(interceptor2);

        OrderMessage message = new OrderMessage();
        CompletableFuture<SendResult<String, Object>> future = mock(CompletableFuture.class);
        SendResult<String, Object> sendResult = mock(SendResult.class);
        when(future.get(10, TimeUnit.SECONDS)).thenReturn(sendResult);
        when(kafkaTemplate.send(any(String.class), any())).thenReturn(future);

        kafkaMQTemplate.send(message);

        // before 正序，after 倒序
        assertEquals("B1->B2->A2->A1->", trace.toString());
    }

    // ==================== addInterceptor / getInterceptors ====================

    /**
     * 测试：添加拦截器、获取拦截器列表
     */
    @Test
    public void testAddAndGetInterceptors() {
        assertTrue(kafkaMQTemplate.getInterceptors().isEmpty());

        KafkaMessageInterceptor interceptor = mock(KafkaMessageInterceptor.class);
        kafkaMQTemplate.addInterceptor(interceptor);

        assertEquals(1, kafkaMQTemplate.getInterceptors().size());
        assertSame(interceptor, kafkaMQTemplate.getInterceptors().get(0));
    }

    // ==================== 测试用的内部消息类 ====================

    @Data
    @EqualsAndHashCode(callSuper = true)
    static class OrderMessage extends AbstractKafkaMessage {
        private Long orderId;
        private String status;
    }

}
