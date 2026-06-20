package cn.iocoder.yudao.framework.mq.constants;

/**
 * @Author: KingCoding
 * @Date: 2026/6/19
 * @Description: mq常量
 */

public interface RabbitMqConstants {

    String REQUEST_ID_HEADER = "requestId";

    interface Exchange {
        /*测试有关的交换机*/
        String DEMO_EXCHANGE = "demo.topic";

        /*异常信息的交换机*/
        String ERROR_EXCHANGE = "error.topic";
    }

    interface Queue {
        String ERROR_QUEUE_TEMPLATE = "error.{}.queue";
    }

    interface Key {
        /*测试有关的 RoutingKey*/
        String DEMO_NEW_KEY = "demo.new";

        /*异常RoutingKey的前缀*/
        String ERROR_KEY_PREFIX = "error.";
    }
}
