package Kafka;

import java.util.List;
import java.util.concurrent.ExecutionException;
import Data.NbaDataApi;

public class MainKafka {

	public static List<String> standingData = null;

	public static void main(String[] args) throws Exception {
		standingData = NbaDataApi.CollectData();
		for (String line : standingData) {
			System.out.println(line);
		}
		System.out.println("DONE!! NBA data download from API");
		
		// Sending data to the Kafka Cluster
		try {			
			DataProduce.sendingData(standingData);
			System.out.println("_________________Data Successfully sending TO KAFKA_________________");
		} catch (ExecutionException e) {
			System.out.println("Kafka sending ERROR:"+e.getMessage());
		}
	}

}
