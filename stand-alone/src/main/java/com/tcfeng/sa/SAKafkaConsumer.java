package com.tcfeng.sa;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.record.TimestampType;

/**
 * 示範如何從Kafka中讀取Avro的資料
 */
public class SAKafkaConsumer {

    private static String KAFKA_BROKER_URL = "ip1:por1,ip2:port2,ip3:port3"; // Kafka集群在那裡?
    private static String SCHEMA_REGISTRY_URL = "http://ip4:port4,http://ip5:port5,http://ip6:port6"; // SchemaRegistry的服務在那裡?
    private static String GROUP_ID = "groupId1"; // *** <-- 修改為自訂義的Key


    public static void main(String[] args) {
        // 步驟1. 設定要連線到Kafka集群的相關設定
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BROKER_URL); // Kafka集群在那裡?
        props.put("group.id", GROUP_ID); // <-- 這就是ConsumerGroup
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // 指定msgKey的反序列化器
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer"); // 指定msgValue的反序列化器
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL); // <-- SchemaRegistry的服務在那裡?
        //props.put("specific.avro.reader", "true"); // <-- 告訴KafkaAvroDeserializer來反序列成Avro產生的specific物件類別
                                                   //     (如果沒有設定, 則都會以GenericRecord方法反序列)
        props.put("auto.offset.reset", "earliest"); // 是否從這個ConsumerGroup尚未讀取的partition/offset開始讀
        props.put("enable.auto.commit", "false");
        // SASL認證
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"username\" password=\"password\";");

        // 步驟2. 產生一個Kafka的Consumer的實例
        Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(props); // msgKey是string, msgValue是Employee
        // 步驟3. 指定想要訂閱訊息的topic名稱
        String topicName = "topic name";
        // 步驟4. 讓Consumer向Kafka集群訂閱指定的topic (每次重起的時候使用seekToListener來移動ConsumerGroup的offset到topic的最前面)
        
        // =====TEST: 從特定時間抓取=====
        // consumer.subscribe(Arrays.asList(topicName));
        // Set<TopicPartition> assignment = new HashSet<>();
        // while (assignment.size() == 0) {
        //     consumer.poll(100);
        //     assignment = consumer.assignment();
        // }
        // Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        // for (TopicPartition tp : assignment) {
        //     timestampToSearch.put(tp, System.currentTimeMillis() - 48 * 3600 * 1000); // 從48小時前開始
        // }
        // Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampToSearch);
        // for(TopicPartition tp: assignment){
        //     OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
        //     if (offsetAndTimestamp != null) {
        //         consumer.seek(tp, offsetAndTimestamp.offset());
        //     }
        // }
        // while (true) {
        //     ConsumerRecords<String, GenericRecord> records = consumer.poll(1000);
        //     for (ConsumerRecord<String, GenericRecord> record : records) {
        //         String topic = record.topic();
        //         int partition = record.partition();
        //         Long offset = record.offset();
        //         GenericRecord msgValue = record.value();
        //         Long syncId = (Long) msgValue.get("SYNC_ID");
        //         if (syncId > 32525120 && syncId < 32525196) {
        //             System.out.println("topic:" + topic + ", partition:" + partition + ", offset:" + offset + ", msgValue:" + msgValue);
        //         }
        //     }
        // }
        // =====


        consumer.subscribe(Arrays.asList(topicName), new SeekToListener(consumer));

        // 步驟5. 持續的拉取Kafka有進來的訊息
        try {
            System.out.println("Start listen incoming messages ...");
            while (true) {
                // 請求Kafka把新的訊息吐出來
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                
                // 如果有任何新的訊息就會進到下面的迭代
                for (ConsumerRecord<String, GenericRecord> record : records){
                    // ** 在這裡進行商業邏輯與訊息處理 **
                    // 取出相關的metadata
                    String topic = record.topic();
                    int partition = record.partition();
                    Long offset = record.offset();
                    TimestampType timestampType = record.timestampType();
                    long timestamp = record.timestamp();
                    // 取出msgKey與msgValue
                    String msgKey = record.key();
                    GenericRecord msgValue = record.value(); //<-- 注意
                    
                    // 秀出metadata與msgKey & msgValue訊息
                    System.out.println("offset:" + offset + ",msgValue:" + msgValue);

                }
                consumer.commitAsync();
            }
        } finally {
            // 步驟6. 如果收到結束程式的訊號時關掉Consumer實例的連線
            consumer.close();
            System.out.println("Stop listen incoming messages");
        }
    }

}

