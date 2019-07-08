package com.kafka.topic;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * description
 *
 * @author jiangbo [jiang.bo@unisinsight.com]
 * @Date 2019/7/8 14:47
 * @since 1.0
 */
public class TopicProcessor {
    //kafka监听端口
    private static final String BROKER_LIST="localhost:9092";

    /**
     *
     * @param topicName   主题名称
     * @param partitionNumber  分区号
     * @param replicaNumber    备份号
     * @param properties
     */
    public static void createTopic(String topicName,int partitionNumber,int replicaNumber,Properties properties){
        try {
            //Kafka客户管理端类
            AdminClient adminClient;
            //设置bootstrap-server:brokers地址,默认9092
            properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,BROKER_LIST);
            //根据配置类创建管理客户端
            adminClient = AdminClient.create(properties);
            //创建一个或多个主题
            NewTopic newTopic = new NewTopic(topicName,partitionNumber,(short) replicaNumber);
            //将所有主题转换为List
            List list = Arrays.asList(newTopic);
            //创建主题
            adminClient.createTopics(Arrays.asList(newTopic));
            //创建完成关闭
            adminClient.close();
            System.out.println("创建主题成功："+topicName);
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {

        }
    }

    public static void main(String[] args){
        createTopic("javatopic",1,1,new Properties());
    }
}
