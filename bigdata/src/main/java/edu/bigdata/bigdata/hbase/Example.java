package edu.bigdata.bigdata.hbase;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

public class Example {

	public static final String TABLE_NAME = "MY_TABLE_NAME_TOO_01";
	public static final String CF_DEFAULT = "DEFAULT_COLUMN_FAMILY";

	public static void createOrOverwrite(Admin admin, HTableDescriptor table)
			throws IOException {
		if (admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		System.out.print(admin.getClusterStatus().getClusterId());
		System.out.print(admin.getClusterStatus().getHBaseVersion());
		System.out.print(admin.getMasterInfoPort());
		
		admin.createTable(table);
	}

	public static void createSchemaTables(Configuration config)
			throws IOException {
		Connection connection = ConnectionFactory.createConnection(config);
		Admin admin = connection.getAdmin();
		HTableDescriptor table = new HTableDescriptor(
				TableName.valueOf(TABLE_NAME));
		//table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.SNAPPY));
		table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.GZ));

		System.out.print("Creating table. ");
		createOrOverwrite(admin, table);
		System.out.println(" Done.");
	}

	public static void modifySchema(Configuration config) throws IOException {
		Connection connection = ConnectionFactory.createConnection(config);
		Admin admin = connection.getAdmin();

		TableName tableName = TableName.valueOf(TABLE_NAME);
		if (admin.tableExists(tableName)) {
			System.out.println("Table does not exist.");
			System.exit(-1);
		}
		HTableDescriptor table = new HTableDescriptor(tableName);

		// Update existing table
		HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
		newColumn.setCompactionCompressionType(Algorithm.GZ);
		newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
		admin.addColumn(tableName, newColumn);

		// Update existing column family
		HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
		existingColumn.setCompactionCompressionType(Algorithm.GZ);
		existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
		table.modifyFamily(existingColumn);
		admin.modifyTable(tableName, table);

		// Disable an existing table
		admin.disableTable(tableName);

		// Delete an existing column family
		admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));

		// Delete a table (Need to be disabled first)
		admin.deleteTable(tableName);
	}

	public static void main(String... args) throws IOException {
		//这个代码只能在集群环境下跑， 他加载了那个jar包内部自己的hbase-default。xml 
		//Configuration config = HBaseConfiguration.create();// Add any necessary configuration files (hbase-site.xml, core-site.xml)
		//config.addResource(new Path("C:\\Users\\IBM_ADMIN\\Documents\\hadoop","hbase-site.xml"));
		//config.addResource(new Path("C:\\Users\\IBM_ADMIN\\Documents\\conf","core-site.xml"));
		//config.addResource(new Path(URI.create("sftp://bigdata:passw0rd@NAMENODE01/home/bigdata/hadoop-2.7.1/etc/hadoop/core-site.xml")));
		//config.addResource(new Path(URI.create("sftp://bigdata:passw0rd@NAMENODE01/home/bigdata/hbase-1.1.2/conf/hbase-site.xml")));
	
		
		Configuration config =  new Configuration();
		config.set("hbase.zookeeper.quorum", "9.115.65.92");
		createSchemaTables(config);
		modifySchema(config);
		
	}
}