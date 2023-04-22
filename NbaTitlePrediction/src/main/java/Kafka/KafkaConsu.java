package Kafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import Data.NbaDataApi;
import HBaseCon.HBaseTableCreator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

public class KafkaConsu {
	transient static Configuration myHadoopConfig;
    public static void main(String[] args) throws Exception {
    	SparkConf sparkConfig = new SparkConf().setAppName("App").setMaster(
				"local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConfig);
		myHadoopConfig = sc.hadoopConfiguration();
		HBaseTableCreator dbActions = HBaseTableCreator.getDB();
		dbActions.createHiveTable();
		
        // Kafka broker configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfig.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConfig.OFFSET_RESET_LATEST);

        // Kafka consumer instance
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to topic(s)
        consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC_NAME));

        int noMessageFound = 0;

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            // If no message found after max noMessageFoundCount, exit the loop
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > KafkaConfig.MAX_NO_MESSAGE_FOUND_COUNT)
                    break;
                else
                    continue;
            }
            // Process messages
            consumerRecords.forEach(record -> {
//            	dbActions.insertMatchDataIntoHBase(myHadoopConfig, rdd);
                System.out.println("------------------NBA STANDING DATA SAFE TO HBASE----------------------");
                List<String> data = new ArrayList<String>();
            	data.add(record.value());
            	System.out.println(data);
                try {
					HBaseTableCreator.insertTestData(data);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            });

            // Commit offsets
            consumer.commitAsync();
        }

        consumer.close();
        System.out.println("Kafka consumer has been closed.");
    }
}
