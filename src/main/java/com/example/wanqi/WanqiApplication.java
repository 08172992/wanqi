package com.example.wanqi;

import com.alibaba.fastjson.JSONArray;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
public class WanqiApplication {

    public static void main(String[] args) throws MqttException {

        SpringApplication.run(WanqiApplication.class, args);
        int qos = 0;
        String broker = "tcp://192.168.25.29:1883";
        String clientId = "emqx_test";
        MemoryPersistence persistence = new MemoryPersistence();
        Properties props = new Properties();
        // kafka地址
        props.put("bootstrap.servers", "192.168.25.31:9092,192.168.25.32:9092,192.168.25.33:9092");
        // 设置消费组
        props.put("group.id", "bigdata");
        // 是否自动提交
        props.put("enable.auto.commit", "true");
        // 设置自动提交时间隔
        props.put("auto.commit.interval.ms", "1000");
        // 设置消费,一般设置earliest或者latest
        props.put("auto.offset.reset", "latest");
        // 序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("alarmData","alarmFile","gps","slopeData","teethDefect"));


        MqttClient client = new MqttClient(broker, clientId, persistence);

        // MQTT 连接选项
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setUserName("emqx_test");
        connOpts.setPassword("emqx_test_password".toCharArray());
        // 保留会话
        connOpts.setCleanSession(true);

        // 设置回调
        client.setCallback(new OnMessageCallback());

        client.connect(connOpts);


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10);
            for (ConsumerRecord<String, String> record : records) {
                if(record.topic().equals("slopeData")){
                    JSONArray test = JSONArray.parseArray(record.value());
                    for (Object val : test) {
                        MqttMessage message = new MqttMessage(val.toString().getBytes());
                        message.setQos(qos);
                        client.publish(record.topic(), message);
                    }
                } else {
                    MqttMessage message = new MqttMessage(record.value().getBytes());
                    message.setQos(qos);
                    client.publish(record.topic(), message);
                }
                System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(),
                        record.offset(), record.key(), record.value());

            }
            consumer.commitAsync();
        }
    }

}
