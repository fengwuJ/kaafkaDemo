package com.kafka.producter;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * description
 *
 * @author jiangbo [jiang.bo@unisinsight.com]
 * @Date 2019/7/8 14:49
 * @since 1.0
 * 生产者生产信息
 */
public class MessageProducer {
    //主题名称
    private static final String TOPIC="javatopic";
    //brokers地址
    private static final String BROKER_LIST="localhost:9092";
    //Kafka生产者类
    private static KafkaProducer<String,String> producer = null;

    //静态加载
    static{
        //创建配置信息
        Properties configs = initConfig();
        //创建生产者
        producer = new KafkaProducer<String, String>(configs);
    }

    private static Properties initConfig(){
        //获取配置类
        Properties properties = new Properties();
        //配置bootstrap-server:brokers地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKER_LIST);
        //配置acks：procedure要求leader在考虑完成请求之前收到的确认数，用于控制发送记录在服务端的持久化，其值可以为如下：
        //acks = 0 如果设置为零，则生产者将不会等待来自服务器的任何确认，该记录将立即添加到套接字缓冲区并视为已发送。在这种情况下，无法保证服务器已收到记录，并且重试配置将不会生效（因为客户端通常不会知道任何故障），为每条记录返回的偏移量始终设置为-1。
        //acks = 1 这意味着leader会将记录写入其本地日志，但无需等待所有副本服务器的完全确认即可做出回应，在这种情况下，如果leader在确认记录后立即失败，但在将数据复制到所有的副本服务器之前，则记录将会丢失。
        //acks = all 这意味着leader将等待完整的同步副本集以确认记录，这保证了只要至少一个同步副本服务器仍然存活，记录就不会丢失，这是最强有力的保证，这相当于acks = -1的设置。
        //可以设置的值为：all, -1, 0, 1
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        //key.serializer:key的Serializer类，实现类实现了接口org.apache.kafka.common.serialization.Serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //value.serializer:值的Serializer类，实现类实现了接口org.apache.kafka.common.serialization.Serializer
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        return properties;
    }

    public static void main(String[] args){
        try{
            //消息内容
            String message = "hello world";
            //Producer的接收对象
            //Topic：主题名称，message：消息内容
            ProducerRecord<String,String> record = new ProducerRecord<String,String>(TOPIC,message);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(null==exception){
                        System.out.println("perfect!");
                    }
                    if(null!=metadata){
                        System.out.print("offset:"+metadata.offset()+";partition:"+metadata.partition());
                    }
                }
            }).get();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }
}
