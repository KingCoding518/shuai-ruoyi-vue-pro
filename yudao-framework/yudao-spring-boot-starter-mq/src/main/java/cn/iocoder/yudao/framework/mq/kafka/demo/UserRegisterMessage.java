package cn.iocoder.yudao.framework.mq.kafka.demo;

import cn.iocoder.yudao.framework.mq.kafka.core.message.AbstractKafkaMessage;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 示例：用户注册消息
 * <p>
 * Topic 默认使用类名，即 "UserRegisterMessage"
 * 如需自定义，重写 getTopic() 即可
 *
 * @author 芋道源码
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class UserRegisterMessage extends AbstractKafkaMessage {

    /**
     * 用户编号
     */
    private Long userId;

    /**
     * 用户名
     */
    private String username;

    /**
     * 注册时间戳
     */
    private Long registerTime;

    // ========== 如需自定义 Topic，重写此方法 ==========
    // @Override
    // @JsonIgnore
    // public String getTopic() {
    //     return "user-register-topic";
    // }

}
