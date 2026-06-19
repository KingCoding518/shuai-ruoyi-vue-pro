package cn.iocoder.yudao.module.infra.controller.admin.demo.MqDemo.mq.producer;

import cn.iocoder.yudao.framework.mq.constants.KafkaConstants;
import cn.iocoder.yudao.framework.mq.kafka.core.KafkaMqHelper;
import cn.iocoder.yudao.framework.tenant.core.aop.TenantIgnore;
import cn.iocoder.yudao.module.infra.controller.admin.demo.MqDemo.mq.message.KafkaDemoMessage;
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
@Tag(name = "kafka生产者demo")
@RestController
@RequestMapping("/infra/mq/kafkaDemoProducer")
@RequiredArgsConstructor
public class KafkaDemoProducerController {

    private final KafkaMqHelper kafkaMqHelper;

    /**
     * 发送一次消息，批量消费
     */
    @GetMapping("/demo1")
    @PermitAll
    @TenantIgnore
    public void demo1() {
        for (int i = 0; i < 100; i++) {
            KafkaDemoMessage kafkaDemoMessage = new KafkaDemoMessage();
            kafkaDemoMessage.setAge(i);
            kafkaDemoMessage.setEmail("邮箱：" + i + "@qq.com");
            kafkaDemoMessage.setName("大帅" + i);
            kafkaMqHelper.send(KafkaConstants.Topic.DEMO_BATCH_TOPIC, kafkaDemoMessage);
        }
    }

    /**
     * 发送一次消息，批量消费
     */
    @GetMapping("/demo2")
    @PermitAll
    @TenantIgnore
    public void demo2() {
        for (int i = 0; i < 100; i++) {
            KafkaDemoMessage kafkaDemoMessage = new KafkaDemoMessage();
            kafkaDemoMessage.setAge(i);
            kafkaDemoMessage.setEmail("邮箱：" + i + "@qq.com");
            kafkaDemoMessage.setName("大帅" + i);
            kafkaMqHelper.send(KafkaConstants.Topic.DEMO_TOPIC, kafkaDemoMessage);
        }
    }
}
