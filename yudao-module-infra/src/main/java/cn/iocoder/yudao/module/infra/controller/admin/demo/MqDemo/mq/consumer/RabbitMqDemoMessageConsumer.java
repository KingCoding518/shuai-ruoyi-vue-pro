package cn.iocoder.yudao.module.infra.controller.admin.demo.MqDemo.mq.consumer;

import cn.iocoder.yudao.framework.mq.constants.RabbitMqConstants;
import cn.iocoder.yudao.module.infra.controller.admin.demo.MqDemo.mq.message.RabbitMqDemoMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * @Author: KingCoding
 * @Date: 2026/6/19
 * @Description:
 */
@Slf4j
@Component
public class RabbitMqDemoMessageConsumer {

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "search.order.pay.queue", durable = "true"),
            exchange = @Exchange(name = RabbitMqConstants.Exchange.DEMO_EXCHANGE, type = ExchangeTypes.TOPIC),
            key = RabbitMqConstants.Key.DEMO_NEW_KEY
    ))
    public void listenOrderPay(RabbitMqDemoMessage message) {
        log.info("[onMessage][收到 KafkaDemoMessage 消息：name={}, age={}, email={}]",
                message.getName(), message.getAge(), message.getEmail());
    }

}
