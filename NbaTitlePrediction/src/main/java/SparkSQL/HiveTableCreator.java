package SparkSQL;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import Data.TeamInfo;

//import DataModel.TeamInfo;
import HBaseCon.ConnectorToHBase;

public class HiveTableCreator {
	public static JavaSparkContext mySparkContext;
	public static final String tableName = "NBAteamInfo";
	private static SparkSession mySparkSession;
	private static final String CF_NAME = "cf";
	private static final String CF_ROW_KEY = "Row Key";
	
    public static void main(String[] args) throws IOException{
    	 Configuration config = HBaseConfiguration.create();
         try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
             Table hTable = connection.getTable(TableName.valueOf(tableName));
             Scan scan = new Scan();
             scan.addColumn(Bytes.toBytes(CF_ROW_KEY), Bytes.toBytes("rowkeyTeamId"));
             ResultScanner scanner = hTable.getScanner(scan);
         
			String keyValue = "";
			char separaterChar = '/';
			List<TeamInfo> teamInfoList = new ArrayList<>();
			for (Result result = scanner.next(); result != null; result = scanner
					.next()){
				keyValue = result.toString().substring(11, result.toString().indexOf(separaterChar));
				TeamInfo perMatchInfoRecord = getRecordByKey(ConnectorToHBase.getConnection(),keyValue);
				teamInfoList.add(perMatchInfoRecord);
				System.out.println(perMatchInfoRecord.toString());
			}
			
			mySparkSession = SparkSession.builder()
				    .appName("HiveTableCreator")
				    .master("local[*]")
				    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
				    .config("hive.metastore.uris", "thrift://quickstart.cloudera:9083")
				    .enableHiveSupport()
				    .getOrCreate();
			mySparkSession.createDataFrame(teamInfoList, TeamInfo.class).createOrReplaceTempView(tableName);
			saveHiveTableData();
	        scanner.close();
	        System.out.println("Hive table created successfully!");
         }
    }
    public static void saveHiveTableData() {
		String query =  " SELECT * FROM " + tableName  +  " ORDER BY Conference,CAST(ConferenceRank as int)";
		Dataset<Row> sqlDataFrame = mySparkSession.sql(query);
		sqlDataFrame.write().mode(SaveMode.Overwrite).saveAsTable(tableName);
		sqlDataFrame.show();
		System.out.println(query+"Hive table inserted successfully!");
	}
    public static TeamInfo getRecordByKey(Connection connection, String key) throws IOException {
		try (Table tb = connection.getTable(TableName.valueOf(tableName))) {
			Get g = new Get(Bytes.toBytes(key));
			Result result = tb.get(g);
			if (result.isEmpty()) {
				return null;
			}
			byte [] value0 = result.getValue(Bytes.toBytes(CF_ROW_KEY),Bytes.toBytes("rowkeyTeamId"));
			byte [] value1 = result.getValue(Bytes.toBytes(CF_NAME),Bytes.toBytes("Season"));
			byte [] value2 = result.getValue(Bytes.toBytes(CF_NAME),Bytes.toBytes("SeasonType"));
			byte [] value3 = result.getValue(Bytes.toBytes(CF_NAME),Bytes.toBytes("Key"));
			byte [] value4 = result.getValue(Bytes.toBytes(CF_NAME),Bytes.toBytes("City"));
			byte [] value5 = result.getValue(Bytes.toBytes(CF_NAME),Bytes.toBytes("Name"));
			byte [] value6 = result.getValue(Bytes.toBytes(CF_NAME),Bytes.toBytes("Conference"));
			byte [] value7 = result.getValue(Bytes.toBytes(CF_NAME),Bytes.toBytes("ConferenceRank"));
			byte [] value8 = result.getValue(Bytes.toBytes(CF_NAME),Bytes.toBytes("Wins"));
			byte [] value9 = result.getValue(Bytes.toBytes(CF_NAME),Bytes.toBytes("Losses"));
			
			TeamInfo returnValue = new TeamInfo(Bytes.toString(value0),Bytes.toString(value1),
					Bytes.toString(value2), Bytes.toString(value3), Bytes.toString(value4),Bytes.toString(value5),
					Bytes.toString(value6), Bytes.toString(value7), Bytes.toString(value8),Bytes.toString(value9)
					);
			return returnValue;
		}
	}
}
