package edu.bigdata.bigdata.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class WriteLargeToStudent {

	public static void main(String[] args) throws IOException {
//		Configuration config = new Configuration();		
//		config.set("hbase.zookeeper.quorum","9.115.65.92");
		Configuration hbaseConfig= HBaseConfiguration.create();
		hbaseConfig.set("hbase.zookeeper.quorum","9.115.65.92");
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
		
		Connection connection = ConnectionFactory.createConnection(hbaseConfig);
		
		Table table= connection.getTable(TableName.valueOf("student"));
//		for (int i= 6; i < 100 ; ++i) {
//			Delete del= new Delete(Bytes.toBytes(i));
//			table.delete(del);
//		}
		
		for (int i= (10000*10000+1); i < 10000*10000*10; ++i) {
			final Put put = new Put(Bytes.toBytes(String.valueOf(i)));  
	        put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes  
	                .toBytes("stu" + i));  
	        
	        put.addColumn(Bytes.toBytes("school"), Bytes.toBytes("name"), Bytes  
	                .toBytes("school" + i));  
	        table.put(put);
	        
		}
		
		
		connection.close();
		
	}

}
