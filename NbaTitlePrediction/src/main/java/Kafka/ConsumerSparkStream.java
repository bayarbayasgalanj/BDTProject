package Kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import HBaseCon.HBaseTableCreator;

public class ConsumerSparkStream {
	public static void main(String[] args) throws Exception {
    	
    	HBaseTableCreator dbHbase = HBaseTableCreator.getDB();
		dbHbase.createHiveTable();
		
    	 String KAFKA_BROKERS = KafkaConfig.KAFKA_BROKERS;
         String TOPIC_NAME = KafkaConfig.TOPIC_NAME;
         String CLIENT_ID = KafkaConfig.CLIENT_ID;
         String GROUP_ID_CONFIG = KafkaConfig.GROUP_ID_CONFIG;
         String OFFSET_RESET = KafkaConfig.OFFSET_RESET_EARLIER;
         
         Map<String, Object> kafkaParams = new HashMap<>();
         kafkaParams.put("bootstrap.servers", KAFKA_BROKERS);
         kafkaParams.put("key.deserializer", StringDeserializer.class);
         kafkaParams.put("value.deserializer", StringDeserializer.class);
         kafkaParams.put("group.id", GROUP_ID_CONFIG);
         kafkaParams.put("client.id", CLIENT_ID);
         kafkaParams.put("auto.offset.reset", OFFSET_RESET);
         kafkaParams.put("max.poll.records", KafkaConfig.MAX_POLL_RECORDS);

         SparkConf conf = new SparkConf().setAppName("KafkaStreamingExample").setMaster("local[*]");
         JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(2000));
         JavaInputDStream<ConsumerRecord<String, String>> stream =
             KafkaUtils.createDirectStream(
                 jssc,
                 LocationStrategies.PreferConsistent(),
                 ConsumerStrategies.<String, String>Subscribe(Collections.singleton(TOPIC_NAME), kafkaParams)
             );
         JavaDStream<String> recordsRDD =
        		 stream.map(record -> {
        			 String lineRecord = record.value();
 					System.out.println("lineRecord "+lineRecord);
 					return lineRecord.toString();
        		 });
         
         recordsRDD.foreachRDD(rdd -> {
     	    rdd.foreach(record -> {
    	        System.out.println("foreachRDD====================="+record.toString()+"++++++++++++++");
     	    	List<String> data = new ArrayList<String>();
            	data.add(record);
            	try {
					HBaseTableCreator.insertTestData(data);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    	    });
    	});
		jssc.start();
        jssc.awaitTermination();
//        sc.close();
    }
}
