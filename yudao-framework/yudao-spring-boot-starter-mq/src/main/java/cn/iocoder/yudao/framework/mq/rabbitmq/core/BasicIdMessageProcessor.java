package cn.iocoder.yudao.framework.mq.rabbitmq.core;

import cn.hutool.core.lang.UUID;
import cn.iocoder.yudao.framework.mq.constants.RabbitMqConstants;
import org.slf4j.MDC;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;


public class BasicIdMessageProcessor implements MessagePostProcessor {
    @Override
    public Message postProcessMessage(Message message) throws AmqpException {
        String requestId = MDC.get(RabbitMqConstants.REQUEST_ID_HEADER);
        if (requestId == null) {
            requestId = UUID.randomUUID().toString(true);
        }
        // 写入RequestID标示
        message.getMessageProperties().setHeader(RabbitMqConstants.REQUEST_ID_HEADER, requestId);
        return message;
    }
}
