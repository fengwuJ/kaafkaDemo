package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * description
 *
 * @author jiangbo [jiang.bo@unisinsight.com]
 * @Date 2019/7/8 14:51
 * @since 1.0
 * 消费者消费信息
 */
public class MessageConsumer {
    //主题名称
    private static final String TOPIC="javatopic";
    //Brokers地址
    private static final String BROKER_LIST="localhost:9092";
    //Kafka消费者
    private static KafkaConsumer<String,String> kafkaConsumer = null;

    //静态加载
    static {
        //初始化配置信息
        Properties properties = initConfig();
        //根据配置信息创建Kafka消费之
        kafkaConsumer = new KafkaConsumer<String, String>(properties);
        //消费者订阅主题
        kafkaConsumer.subscribe(Arrays.asList(TOPIC));
    }

    private static Properties initConfig(){
        //创建配置类
        Properties properties = new Properties();
        //配置bootstrap-server:brokers地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKER_LIST);
        //group-id:Consumer归属的组ID，broker是根据group.id来判断是队列模式还是发布订阅模式，非常重要
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        //client-id:一个用于跟踪调查的ID ，最好同group.id相同
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"test");
        //key.deserializer:键的反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //value.deserializer:值得反序列化
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        return properties;
    }

    public static void main(String[] args){
        try{
            while(true){
                //同ProducerRecords
                ConsumerRecords<String,String> records = kafkaConsumer.poll(100);
                //收到的订阅消息集合
                for(ConsumerRecord record:records){
                    //遍历消息
                    try{
                        System.out.println(record.value());
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }

        }catch(Exception e){
            e.printStackTrace();
        }finally {
            kafkaConsumer.close();
        }
    }
}
