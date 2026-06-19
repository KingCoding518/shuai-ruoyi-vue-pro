package cn.iocoder.yudao.module.infra.controller.admin.demo.MqDemo.mq.message;

import lombok.Data;

/**
 * @Author: KingCoding
 * @Date: 2026/6/19
 * @Description: kafka测试消息
 */
@Data
public class KafkaDemoMessage {

    private String name;

    private Integer age;

    private String email;
}
