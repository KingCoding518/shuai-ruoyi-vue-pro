package cn.iocoder.yudao.framework.mq.rabbitmq.config;

import cn.hutool.core.util.StrUtil;
import cn.iocoder.yudao.framework.mq.constants.RabbitMqConstants;
import cn.iocoder.yudao.framework.mq.rabbitmq.core.RabbitMqHelper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.MDC;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.ContainerCustomizer;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * RabbitMQ 消息队列配置类
 *
 * @author 芋道源码
 */
@AutoConfiguration
@ConditionalOnClass(value = {MessageConverter.class, AmqpTemplate.class})
public class YudaoRabbitMQAutoConfiguration implements EnvironmentAware {

    private String defaultErrorRoutingKey;
    private String defaultErrorQueue;

    /**
     * rabbitListenerContainerFactory 这是 rabbit 默认的监听工厂，代码中声明会覆盖掉 源码中的这个bean的
     * @ConditionalOnProperty(
     *     prefix = "spring.rabbitmq.listener",  // 配置前缀
     *     name = "type",                         // 配置名
     *     havingValue = "simple",               // 当配置值等于 "simple" 时生效
     *     matchIfMissing = true                  // 如果没配这个属性，默认也生效
     * )
     * @param configurer
     * @param connectionFactory
     * @param simpleContainerCustomizer
     * @return
     */
    @Bean(name = "rabbitListenerContainerFactory")
    @ConditionalOnProperty(prefix = "spring.rabbitmq.listener", name = "type", havingValue = "simple",
            matchIfMissing = true)
    SimpleRabbitListenerContainerFactory simpleRabbitListenerContainerFactory(
            SimpleRabbitListenerContainerFactoryConfigurer configurer, ConnectionFactory connectionFactory,
            ObjectProvider<ContainerCustomizer<SimpleMessageListenerContainer>> simpleContainerCustomizer) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        simpleContainerCustomizer.ifUnique(factory::setContainerCustomizer);
        factory.setAfterReceivePostProcessors(message -> {
            Object header = message.getMessageProperties().getHeader(RabbitMqConstants.REQUEST_ID_HEADER);
            if (header != null) {
                MDC.put(RabbitMqConstants.REQUEST_ID_HEADER, header.toString());
            }
            return message;
        });
        return factory;
    }

    /**
     * 注入 IOC 中的名称一般是方法名 首字母变小写，例如：下面是：messageConverter
     *
     * @param mapper
     * @return
     */
    @Bean
    public MessageConverter messageConverter(ObjectMapper mapper) {
        // 1.定义消息转换器
        Jackson2JsonMessageConverter jackson2JsonMessageConverter = new Jackson2JsonMessageConverter(mapper);
        // 2.配置自动创建消息id，用于识别不同消息
        jackson2JsonMessageConverter.setCreateMessageIds(true);
        return jackson2JsonMessageConverter;
    }

    /**
     * <h1>消息处理失败的重试策略</h1>
     * 本地重试失败后，消息投递到专门的失败交换机和失败消息队列：error.queue
     */
    @Bean
    @ConditionalOnClass(MessageRecoverer.class)
    @ConditionalOnMissingBean
    public MessageRecoverer republishMessageRecoverer(RabbitTemplate rabbitTemplate) {
        // 消息处理失败后，发送到错误交换机：error.direct，RoutingKey默认是error.微服务名称
        return new RepublishMessageRecoverer(
                rabbitTemplate, RabbitMqConstants.Exchange.ERROR_EXCHANGE, defaultErrorRoutingKey);
    }

    /**
     * rabbitmq发送工具
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(RabbitTemplate.class)
    public RabbitMqHelper rabbitMqHelper(RabbitTemplate rabbitTemplate) {
        return new RabbitMqHelper(rabbitTemplate);
    }

    /**
     * 专门接收处理失败的消息
     * 项目运行的时候就创建 处理错误的交换机、队列，并且绑定关系
     */
    @Bean
    public DirectExchange errorMessageExchange() {
        return new DirectExchange(RabbitMqConstants.Exchange.ERROR_EXCHANGE);
    }

    @Bean
    public Queue errorQueue() {
        return new Queue(defaultErrorQueue, true);
    }

    @Bean
    public Binding errorBinding(Queue errorQueue, DirectExchange errorMessageExchange) {
        return BindingBuilder.bind(errorQueue).to(errorMessageExchange).with(defaultErrorRoutingKey);
    }

    @Override
    public void setEnvironment(Environment environment) {
        String appName = environment.getProperty("spring.application.name");
        this.defaultErrorRoutingKey = RabbitMqConstants.Key.ERROR_KEY_PREFIX + appName;
        this.defaultErrorQueue = StrUtil.format(RabbitMqConstants.Queue.ERROR_QUEUE_TEMPLATE, appName);
    }

}
