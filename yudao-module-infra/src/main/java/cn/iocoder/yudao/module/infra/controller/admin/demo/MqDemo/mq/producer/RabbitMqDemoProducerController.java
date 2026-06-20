package cn.iocoder.yudao.module.infra.controller.admin.demo.MqDemo.mq.producer;

import cn.iocoder.yudao.framework.mq.constants.RabbitMqConstants;
import cn.iocoder.yudao.framework.mq.rabbitmq.core.RabbitMqHelper;
import cn.iocoder.yudao.framework.tenant.core.aop.TenantIgnore;
import cn.iocoder.yudao.module.infra.controller.admin.demo.MqDemo.mq.message.RabbitMqDemoMessage;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.PermitAll;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 正常情况下这应该是一个 service 类，为了方便测试把它搞成 controller 类
 *
 * @Author: KingCoding
 * @Date: 2026/6/19
 * @Description:
 */
@Tag(name = "rabbitmq生产者demo")
@RestController
@RequestMapping("/infra/mq/rabbitmq/demoProducer")
@RequiredArgsConstructor
public class RabbitMqDemoProducerController {

    private final RabbitMqHelper rabbitMqHelper;

    /**
     * 发送一次消息，批量消费
     */
    @GetMapping("/demo1")
    @PermitAll
    @TenantIgnore
    public void demo1() {
        for (int i = 0; i < 100; i++) {
            RabbitMqDemoMessage rabbitMqDemoMessage = new RabbitMqDemoMessage();
            rabbitMqDemoMessage.setAge(i);
            rabbitMqDemoMessage.setEmail("邮箱：" + i + "@qq.com");
            rabbitMqDemoMessage.setName("大帅" + i);
            rabbitMqHelper.send(
                    RabbitMqConstants.Exchange.DEMO_EXCHANGE,
                    RabbitMqConstants.Key.DEMO_NEW_KEY,
                    rabbitMqDemoMessage
                    );
        }
    }
}
