package com.example.wanqi;

import com.alibaba.fastjson.JSONArray;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootApplication
public class WanqiApplication {

    private static final String MQTT_BROKER = "tcp://192.168.25.29:1883";
    private static final String MQTT_CLIENT_ID = "mqtt_client";
    private static final int MQTT_QOS = 0;
    private static final int MAX_CONCURRENT_THREADS = 20;

    public static void main(String[] args)  {

        SpringApplication.run(WanqiApplication.class, args);
        // Set up MQTT client
        MqttClient mqttClient = null;
        try {
            mqttClient = new MqttClient(MQTT_BROKER, MQTT_CLIENT_ID, new MemoryPersistence());
            MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
            mqttConnectOptions.setCleanSession(true);
            mqttConnectOptions.setAutomaticReconnect(true);
            mqttClient.connect(mqttConnectOptions);
        } catch (MqttException e) {
            e.printStackTrace();
        }

        // Set up Kafka consumer
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.25.31:9092,192.168.25.32:9092,192.168.25.33:9092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata1");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Arrays.asList("cumt_gps","personLocation","watchLocation",
                "watchOtherData","wqCarLocation","wqPersonLocation","alarmData","gps","slopeData","teethDefect","wqEnvMonitor"));

        // Set up thread pool for concurrent consumption
        ExecutorService executorService = Executors.newFixedThreadPool(MAX_CONCURRENT_THREADS);

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                MqttClient finalMqttClient = mqttClient;
                executorService.submit(() -> {
                    // Process record and publish to MQTT
                    String message = record.value();
                    MqttMessage mqttMessage = null;
                    try {
                        mqttMessage = new MqttMessage(message.getBytes("UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        throw new RuntimeException(e);
                    }
                    mqttMessage.setQos(MQTT_QOS);
                    try {
                        finalMqttClient.publish(record.topic(), mqttMessage);
                        System.out.println("11111111"+message);
                    } catch (MqttException e) {
                        e.printStackTrace();
                    }
                });
            }
            kafkaConsumer.commitAsync();
        }
    }


//        for (String s : Arrays.asList("cumt_gps", "personLocation", "watchLocation", "watchOtherData",
//                "wqCarLocation", "wqPersonLocation", "alarmData", "gps", "slopeData", "teethDefect")) {
//            MyThread thread = new MyThread(s);
//            thread.start();
//        }


//        int qos = 0;
//        String broker = "tcp://192.168.25.29:1883";
//        String clientId = "emqx_test";
//        MemoryPersistence persistence = new MemoryPersistence();
//        Properties props = new Properties();
//        // kafka地址
//        props.put("bootstrap.servers", "192.168.25.31:9092,192.168.25.32:9092,192.168.25.33:9092");
//        // 设置消费组
//        props.put("group.id", "bigdata");
//        props.put("max.poll.interval.ms","86400000");
//        // 设置消费,一般设置earliest或者latest
//        props.put("auto.offset.reset", "latest");
//        // 序列化
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
//        consumer.subscribe(Arrays.asList("cumt_gps","personLocation","watchLocation",
//                "watchOtherData","wqCarLocation","wqPersonLocation","alarmData","gps","slopeData","teethDefect"));
//
//
//        MqttClient client = new MqttClient(broker, clientId, persistence);
//
//        // MQTT 连接选项
//        MqttConnectOptions connOpts = new MqttConnectOptions();
////        connOpts.setAutomaticReconnect(true);
//        // 保留会话
//        connOpts.setCleanSession(true);
//
//        // 设置回调
//        client.setCallback(new MqttCallback() {
//            @Override
//            public void connectionLost(Throwable throwable) {
//                // 连接断开
//                System.out.println("MQTT将在1秒后重连");
//                while(true) {
//                    try {
//                        Thread.sleep(1000);
//                        // 重新连接
//                        client.connect(connOpts);
//                        break;
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                        continue;
//                    }
//                }
//                while (true) {
//                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
//                        for (ConsumerRecord<String, String> record : records) {
//                            MqttMessage message = null;
//                            try {
//                                message = new MqttMessage(record.value().getBytes("UTF-8"));
//                            } catch (UnsupportedEncodingException e) {
//                                throw new RuntimeException(e);
//                            }
//                            message.setQos(qos);
//                            try {
//                                client.publish(record.topic(), message);
//                            } catch (MqttException e) {
//                                throw new RuntimeException(e);
//                            }
//                            System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(),
//                                    record.offset(), record.key(), record.value());
//                            try {
//                                Thread.sleep(50);
//                            } catch (InterruptedException e) {
//                                throw new RuntimeException(e);
//                            }
//                        }
//                        consumer.commitAsync();
//                }
//            }
//
//            @Override
//            public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
//                System.out.println("Topic: " + s + " Message: " + mqttMessage.toString().getBytes("UTF-8"));
//            }
//
//            @Override
//            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
//            }
//
//
//        });
//
//        client.connect(connOpts);
//
//        while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
//                for (ConsumerRecord<String, String> record : records) {
//                    MqttMessage message = new MqttMessage(record.value().getBytes("UTF-8"));
//                    message.setQos(qos);
//                    client.publish(record.topic(), message);
//                    System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(),
//                            record.offset(), record.key(), record.value());
//                    Thread.sleep(50);
//                }
//                consumer.commitAsync();
//        }
    }

