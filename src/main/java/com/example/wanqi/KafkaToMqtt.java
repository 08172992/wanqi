package com.example.wanqi;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Properties;

public class KafkaToMqtt implements MqttCallback {

    private MqttClient mqttClient;
    private String mqttBrokerUrl;
    private String mqttClientId;
    private String mqttTopic;
    private int mqttQos;
    private String kafkaBrokerUrl;
    private String kafkaGroupId;
    private String kafkaTopic;

    public KafkaToMqtt(String mqttBrokerUrl, String mqttClientId, String mqttTopic, int mqttQos, String kafkaBrokerUrl, String kafkaGroupId, String kafkaTopic) {
        this.mqttBrokerUrl = mqttBrokerUrl;
        this.mqttClientId = mqttClientId;
        this.mqttTopic = mqttTopic;
        this.mqttQos = mqttQos;
        this.kafkaBrokerUrl = kafkaBrokerUrl;
        this.kafkaGroupId = kafkaGroupId;
        this.kafkaTopic = kafkaTopic;
    }

    public void start() throws MqttException, UnsupportedEncodingException, InterruptedException {
        // Set up MQTT client
        mqttClient = new MqttClient(mqttBrokerUrl, mqttClientId, new MemoryPersistence());
        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setCleanSession(true);
        mqttConnectOptions.setAutomaticReconnect(true);
        mqttClient.connect(mqttConnectOptions);
        mqttClient.setCallback(this);

        // Set up Kafka consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerUrl);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(kafkaTopic));

        // Start consuming and publishing
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String message = record.value();
                MqttMessage mqttMessage = new MqttMessage(message.getBytes("UTF-8"));
                mqttMessage.setQos(mqttQos);
                mqttClient.publish(mqttTopic, mqttMessage);
                System.out.println("hhhhhhhhhhh:"+message);
            }
            Thread.sleep(10);
        }
    }

    @Override
    public void connectionLost(Throwable throwable) {
        // Handle connection lost
    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        // Handle incoming MQTT message
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
        // Handle message delivery complete
    }
}
