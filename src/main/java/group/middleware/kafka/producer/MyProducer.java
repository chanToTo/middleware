package group.middleware.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author orangeC
 * @description
 * @date 2020/2/11 11:08
 */
public class MyProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        // 指定连接的kafka集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        // ack应答级别
        properties.put("acks","all");
        // 重试次数
        properties.put("retries",1);
        // 一次发送16k数据，批次大小
        properties.put("batch.size","16384");
        // 一毫秒会发送，等待时间
        properties.put("linger.ms",1);
        // RecordAccumulator缓冲区大小 ，能存32M
        properties.put("buffer.memory",33554432);

        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // 添加分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"group.middleware.kafka.partitioner.MyPartitioner");


        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String,String>("first", "hello " + i));
        }
        producer.close();

    }
}
