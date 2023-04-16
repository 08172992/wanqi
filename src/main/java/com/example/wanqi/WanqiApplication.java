package com.example.wanqi;

import com.alibaba.fastjson.JSONArray;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
public class WanqiApplication {

    public static void main(String[] args) throws MqttException, UnsupportedEncodingException, InterruptedException {

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
        props.put("max.poll.interval.ms","86400000");
        // 设置消费,一般设置earliest或者latest
        props.put("auto.offset.reset", "latest");
        // 序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("cumt_gps","personLocation","watchLocation",
                "watchOtherData","wqCarLocation","wqPersonLocation","alarmData","gps","slopeData","teethDefect"));


        MqttClient client = new MqttClient(broker, clientId, persistence);

        // MQTT 连接选项
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setAutomaticReconnect(true);
        // 保留会话
        connOpts.setCleanSession(true);

        // 设置回调
        client.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {
                // 连接断开
                System.out.println("MQTT将在1秒后重连");
                while(true) {
                    try {
                        Thread.sleep(1000);
                        // 重新连接
                        client.connect(connOpts);
                        break;
                    } catch (Exception e) {
                        e.printStackTrace();
                        continue;
                    }
                }
                while (true) {
                    if (client.isConnected()){
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                        for (ConsumerRecord<String, String> record : records) {
                            MqttMessage message = null;
                            try {
                                message = new MqttMessage(record.value().getBytes("UTF-8"));
                            } catch (UnsupportedEncodingException e) {
                                throw new RuntimeException(e);
                            }
                            message.setQos(qos);
                            try {
                                client.publish(record.topic(), message);
                            } catch (MqttException e) {
                                throw new RuntimeException(e);
                            }
                            System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(),
                                    record.offset(), record.key(), record.value());
                            try {
                                Thread.sleep(25);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        consumer.commitAsync();
                    } else {
                        consumer.enforceRebalance();
                        consumer.close();
                        break;
                    }
                }
            }

            @Override
            public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                System.out.println("Topic: " + s + " Message: " + mqttMessage.toString().getBytes("UTF-8"));
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            }


        });

        client.connect(connOpts);

        while (true) {
            if (client.isConnected()){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    MqttMessage message = new MqttMessage(record.value().getBytes("UTF-8"));
                    message.setQos(qos);
                    client.publish(record.topic(), message);
                    System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(),
                            record.offset(), record.key(), record.value());
                    Thread.sleep(25);
                }
                consumer.commitAsync();
            } else {
                consumer.enforceRebalance();
                consumer.close();
                break;
            }
        }
    }

}
