package cn.iocoder.yudao.framework.mq.kafka.demo;

import cn.iocoder.yudao.framework.mq.kafka.core.KafkaMQTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 示例：用户注册消息生产者
 *
 * 直接注入 {@link KafkaMQTemplate} 即可发送消息，
 * Topic 由消息类自动决定（默认类名）
 *
 * @author 芋道源码
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class UserRegisterProducer {

    private final KafkaMQTemplate kafkaMQTemplate;

    /**
     * 同步发送用户注册消息（默认 10 秒超时）
     */
    public void sendSync(Long userId, String username) {
        UserRegisterMessage message = new UserRegisterMessage();
        message.setUserId(userId);
        message.setUsername(username);
        message.setRegisterTime(System.currentTimeMillis());
        // 可选：添加自定义 header
        message.addHeader("source", "admin-web");

        try {
            kafkaMQTemplate.send(message);
            log.info("[sendSync][发送用户注册消息成功] userId={}, username={}", userId, username);
        } catch (Exception e) {
            log.error("[sendSync][发送用户注册消息失败] userId={}, username={}", userId, username, e);
            throw e;
        }
    }

    /**
     * 同步发送，自定义超时时间
     */
    public void sendSyncWithTimeout(Long userId, String username, long timeout, TimeUnit unit) {
        UserRegisterMessage message = new UserRegisterMessage();
        message.setUserId(userId);
        message.setUsername(username);
        message.setRegisterTime(System.currentTimeMillis());

        kafkaMQTemplate.send(message, timeout, unit);
    }

    /**
     * 异步发送（不等待结果）
     */
    public void sendAsync(Long userId, String username) {
        UserRegisterMessage message = new UserRegisterMessage();
        message.setUserId(userId);
        message.setUsername(username);
        message.setRegisterTime(System.currentTimeMillis());

        kafkaMQTemplate.sendAsync(message);
        log.info("[sendAsync][提交异步发送] userId={}, username={}", userId, username);
    }

    /**
     * 发送到自定义 Topic（覆盖消息类的默认 Topic）
     */
    public void sendToCustomTopic(String topic, Long userId, String username) {
        UserRegisterMessage message = new UserRegisterMessage();
        message.setUserId(userId);
        message.setUsername(username);
        message.setRegisterTime(System.currentTimeMillis());

        // 发送到自定义 Topic，而不是消息类默认的 "UserRegisterMessage"
        kafkaMQTemplate.sendAsync(topic, message);
        log.info("[sendToCustomTopic][提交异步发送] topic={}, userId={}", topic, userId);
    }

}
