package cn.iocoder.yudao.framework.mq.kafka.demo;

import cn.iocoder.yudao.framework.mq.kafka.core.message.AbstractKafkaMessageListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 示例：用户注册消息消费者
 * <p>
 * 泛型指定消息类型 {@link UserRegisterMessage}，
 * 自动解析出 Topic = "UserRegisterMessage"，
 * Consumer Group = ${spring.application.name}
 *
 * @author 芋道源码
 */
@Slf4j
@Component
public class UserRegisterConsumer extends AbstractKafkaMessageListener<UserRegisterMessage> {

    @Override
    public void onMessage(UserRegisterMessage message) {
        log.info("[onMessage][收到用户注册消息] userId={}, username={}, registerTime={}",
                message.getUserId(), message.getUsername(), message.getRegisterTime());

        // TODO 你的业务逻辑，例如：
        // 1. 发送欢迎邮件
        // 2. 初始化用户数据
        // 3. 赠送注册积分
    }

    // ========== 如需自定义 Consumer Group，重写 group 属性 ==========
    // 方式一：直接覆盖字段（需要在构造后设置，不推荐）
    //
    // 方式二：使用 @Value 注入自定义值
    // @Value("${yudao.kafka.user-register-consumer-group:${spring.application.name}-user-register}")
    // private String group;

}
