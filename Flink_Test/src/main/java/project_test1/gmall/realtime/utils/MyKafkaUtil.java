package project_test1.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    private static String kafkaServer = "192.168.2.21:9092,192.168.2.22:9092,192.168.2.26:9092" ;

    //封装Kafka消费者
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId) {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId) ;
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer) ;
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),prop) ;
    }

    //封装生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {

        return  new FlinkKafkaProducer<String>(kafkaServer,topic,new SimpleStringSchema());
    }


}
