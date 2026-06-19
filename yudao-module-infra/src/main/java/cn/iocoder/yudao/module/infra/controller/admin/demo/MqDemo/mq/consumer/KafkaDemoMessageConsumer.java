package cn.iocoder.yudao.module.infra.controller.admin.demo.MqDemo.mq.consumer;

import cn.iocoder.yudao.framework.mq.constants.KafkaConstants;
import cn.iocoder.yudao.module.infra.controller.admin.demo.MqDemo.mq.message.KafkaDemoMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: KingCoding
 * @Date: 2026/6/19
 * @Description:
 */
@Slf4j
@Component
public class KafkaDemoMessageConsumer {

    private final AtomicInteger batchCounter = new AtomicInteger(0);
    private final AtomicInteger totalCounter = new AtomicInteger(0);

    /**
     * 引入自定义工厂进行批量消费
     * 把一批数据收集完之后，再发送消费者进行消费
     *
     * @param messageList
     */
    @KafkaListener(
            topics = KafkaConstants.Topic.DEMO_BATCH_TOPIC,
            groupId = "demo-consumer-group",
            containerFactory = "kafkaBatchContainerFactory"
    )
    public void handleDemo1(List<KafkaDemoMessage> messageList) {
        int batchNo = batchCounter.incrementAndGet();
        int size = messageList.size();
        int total = totalCounter.addAndGet(size);
        log.info("[handleDemo1][第{}次 poll，本次 {} 条，累计 {} 条]", batchNo, size, total);
        messageList.forEach(message -> {
            log.info("[onMessage][收到 KafkaDemoMessage 消息：name={}, age={}, email={}]",
                    message.getName(), message.getAge(), message.getEmail());
        });
    }

    @KafkaListener(topics = KafkaConstants.Topic.DEMO_TOPIC, groupId = "demo-consumer-group")
    public void handleDemo2(KafkaDemoMessage message) {

        log.info("[onMessage][收到 KafkaDemoMessage 消息：name={}, age={}, email={}]",
                message.getName(), message.getAge(), message.getEmail());

    }
}
