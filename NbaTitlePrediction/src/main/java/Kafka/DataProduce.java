package Kafka;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class DataProduce {

	public static Producer<Long, String> myProducer;

	public static void sendingData(List<String> standingData) throws InterruptedException,
			ExecutionException {

		// preparing property
		Properties myProperty = new Properties();
		myProperty.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
				KafkaConfig.MESSAGE_SIZE);
		myProperty.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		myProperty.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				LongSerializer.class.getName());
		myProperty.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				KafkaConfig.KAFKA_BROKERS);

		myProducer = new KafkaProducer<Long, String>(myProperty);
		
		System.out.println("myProperty"+myProperty);

		System.out.println("myProducer"+myProducer);
		//sending the data to Kafka Cluster
		for (String record : standingData) {
			System.out.println("RECORD"+record);
			ProducerRecord<Long, String> teamRecords = new ProducerRecord<>(
					KafkaConfig.TOPIC_NAME, record);
			System.out.println("matchRecords"+teamRecords);
			myProducer.send(teamRecords).get();
		}
	}
}
