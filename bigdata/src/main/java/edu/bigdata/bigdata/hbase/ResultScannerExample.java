/*
 * Copyright 2014 Nick Dimiduk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.bigdata.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.org.apache.bcel.internal.generic.NEW;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ResultScannerExample extends Configured implements Tool {

	@Deprecated
  /** The identifier for the application table. */
  private static final TableName TABLE_NAME = TableName.valueOf("didi:order_statistics");

	@Deprecated
  public int run(String[] argv) throws IOException {
	Configuration con = getConf();
	con.set("hbase.zookeeper.quorum", "9.115.65.92");
	  
    setConf(HBaseConfiguration.create(getConf()));

    /** Connection to the cluster. A single connection shared by all application threads. */
    Connection connection = null;
    /** A lightweight handle to a specific table. Used from a single thread. */
    Table table = null;
    try {
      // establish the connection to the cluster.
      connection = ConnectionFactory.createConnection(getConf());
      
      // retrieve a handle to the target table.
      table = connection.getTable(TABLE_NAME);
    
     
      
      ResultScanner scanner = table.getScanner(Bytes.toBytes("118"));
      for ( Iterator<Result> it = scanner.iterator(); it.hasNext(); ) {
    	  Result rs = it.next();
    	 
    	  List<Cell> cells = rs.getColumnCells(Bytes.toBytes("118"),Bytes.toBytes("requst"));
    	  for ( Cell cell: cells) {
    		  System.out.println(new String(CellUtil.cloneValue(cell)));
    	  }
    	  
    	  Cell cell = rs.getColumnLatestCell(Bytes.toBytes("118"),Bytes.toBytes("response"));
    	  System.out.println(new String(CellUtil.cloneValue(cell)));
      }
     
    } finally {
      // close everything down
      if (table != null) table.close();
      if (connection != null) connection.close();
    }
    return 0;
  }

  public static void main(String[] argv) throws Exception {
    int ret = ToolRunner.run(new ResultScannerExample(), argv);
    System.exit(ret);
  }
}
