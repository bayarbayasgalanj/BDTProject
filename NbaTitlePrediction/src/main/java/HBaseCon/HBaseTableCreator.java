package HBaseCon;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import Data.NbaDataApi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;
public class HBaseTableCreator  implements Serializable{
	private static final long serialVersionUID = 1L;
    private static final String TABLE_NAME = "NBAteamInfo";
    private static final String CF_ROW_KEY = "Row Key";
    private static final String CF_NAME = "cf";
    private static HBaseTableCreator dbConst;
    
    public static HBaseTableCreator getDB() {
		if (dbConst == null) {
			dbConst = new HBaseTableCreator();
		}
		return dbConst;
	}
    public static void createHiveTable() throws Exception {
    	// Create a configuration object for HBase
        Configuration hbaseConfig = HBaseConfiguration.create();

        // Set the ZooKeeper quorum server and client port
        hbaseConfig.set("hbase.zookeeper.quorum", "localhost");
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");

        // Create a connection to HBase
        Connection hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);

        // Get an admin instance to create tables
        Admin hbaseAdmin = hbaseConnection.getAdmin();
        
        // Create a column family descriptor for the new table
        ColumnFamilyDescriptor cfDescriptor = ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(CF_NAME));
        ColumnFamilyDescriptor cfDescriptorR_Key = ColumnFamilyDescriptorBuilder.of(Bytes.toBytes(CF_ROW_KEY));

        // Create a table descriptor for the new table
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME))
                .setColumnFamily(cfDescriptorR_Key)
                .addColumnFamily(cfDescriptor)
                .build();
        // Create the table with the given table descriptor
        try {
            hbaseAdmin.createTable(tableDescriptor);
            System.out.println("HBase table created: " + TABLE_NAME);
            
            
        } catch (TableExistsException e) {
        	// If existing Table delete Table
        	hbaseAdmin.disableTable(TableName.valueOf(TABLE_NAME));
            hbaseAdmin.deleteTable(TableName.valueOf(TABLE_NAME));
            hbaseAdmin.createTable(tableDescriptor);

            System.out.println("HBase table already exists: " + TABLE_NAME+"\n"+e);
        }
        // Close the HBase admin and connection objects
        hbaseAdmin.close();
        hbaseConnection.close();
    }
    public static void main(String[] args) throws Exception {
    	createHiveTable();
    	insertTestDataPrepare();
    	System.out.println("Test Data INSERTED");
    }
    public static void insertTestDataPrepare() throws Exception{
    	List<String> collectedData = NbaDataApi.CollectData();
    	System.out.println(collectedData);
        insertTestData(collectedData);
    }
    public static void insertTestData(List<String> teamInfos) throws IOException{
    	Configuration hbaseConfig = HBaseConfiguration.create();
        // Set the ZooKeeper quorum server and client port
        hbaseConfig.set("hbase.zookeeper.quorum", "localhost");
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
        // Create a connection to HBase
        Connection hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
    	 // Get a table instance
        Table table = hbaseConnection.getTable(TableName.valueOf(TABLE_NAME));
        // Create a row key
        List<Put> puts = new ArrayList<Put>();
        for (String rowS : teamInfos) {
        	String[] row = rowS.split("\t");
        	Put put = new Put(row[0].getBytes());
        	put.addColumn(CF_ROW_KEY.getBytes(), "rowkeyTeamId".getBytes(), row[0].getBytes());
        	put.addColumn(CF_NAME.getBytes(), "Season".getBytes(), row[1].getBytes());
        	put.addColumn(CF_NAME.getBytes(), "SeasonType".getBytes(), row[2].getBytes());
        	put.addColumn(CF_NAME.getBytes(), "Key".getBytes(), row[3].getBytes());
        	put.addColumn(CF_NAME.getBytes(), "City".getBytes(), row[4].getBytes());
        	put.addColumn(CF_NAME.getBytes(), "Name".getBytes(), row[5].getBytes());
        	put.addColumn(CF_NAME.getBytes(), "Conference".getBytes(), row[6].getBytes());
        	put.addColumn(CF_NAME.getBytes(), "ConferenceRank".getBytes(), row[7].getBytes());
        	put.addColumn(CF_NAME.getBytes(), "Wins".getBytes(), row[8].getBytes());
        	put.addColumn(CF_NAME.getBytes(), "Losses".getBytes(), row[9].getBytes());
        	puts.add(put);
        }
        table.put(puts);
    }
}

