package br.ecomerce.main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class FraudeDetectorService {

    public static void main(String[] args) {
        var fraudService = new FraudeDetectorService();
        var service = new KafkaService(FraudeDetectorService.class.getSimpleName(),"ECOMERCE_NEW_ORDER",fraudService::parse);
        service.run();
    }
    private void parse(ConsumerRecord<String, String> record) {
            System.out.println("--------------------------------------------");
            System.out.println("Processing new order, Checking for fraud ");
            System.out.println(record.key());
            System.out.println(record.value());
            System.out.println(record.partition());
            System.out.println(record.offset());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                //ignoring
                e.printStackTrace();
            }
            System.out.println("Order Processed!");
        }


    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,FraudeDetectorService.class.getSimpleName());

        return properties;
    }
}
