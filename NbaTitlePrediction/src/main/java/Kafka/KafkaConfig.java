package Kafka;

public interface KafkaConfig {

	public static String KAFKA_BROKERS = "quickstart.cloudera:9092";
	public static String TOPIC_NAME = "NbaTeamStanding";
	public static String CLIENT_ID = "clientNBA";
	public static String GROUP_ID_CONFIG = "ConsumerGroupNBAgetDATA";
	public static String OFFSET_RESET_LATEST = "latest";
	public static String OFFSET_RESET_EARLIER = "earliest";
	public static Integer MAX_POLL_RECORDS = 1;
	public static Integer MESSAGE_COUNT = 1000;
	public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 10;
	public static Integer MESSAGE_SIZE = 100000000;

}