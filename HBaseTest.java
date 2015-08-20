package com.test.hbase;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.ws.rs.PUT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.thrift.generated.Hbase.Processor.createTable;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseTest {
	public Configuration conf = HBaseConfiguration.create();
	
	public void createTable(String tableName, String family) {	
		System.out.println("Creating Table...");
		try {			
			HBaseAdmin admin = new HBaseAdmin(conf);
			if(admin.tableExists(tableName)) {
				System.out.println("Table Exists");
				return;
			}
			HTableDescriptor descriptor = new HTableDescriptor(tableName);
			HColumnDescriptor colDescriptor = new HColumnDescriptor(family);
			descriptor.addFamily(colDescriptor);
			admin.createTable(descriptor);
			
			System.out.println("Create Table Done");
			
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void addRecord(String tableName, String rowKey, String family, String qualifier, byte[] data) {
		try {
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), data);
			table.put(put);
			System.out.println("Add Record Done");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		HBaseTest hbTest = new HBaseTest();
		String tableName = "BigData";
		String familyName = "fileContent";
		String qualifier = "content";
		String id = "Z";
		
		hbTest.createTable(tableName, familyName);
		
		Path path = Paths.get("NASDAQ_daily_prices_Z.csv");
		byte[] data;
		
		try {
			data = Files.readAllBytes(path);
			
			long startTime=System.currentTimeMillis();
			
			hbTest.addRecord(tableName, id, familyName, qualifier, data);
			
			long endTime=System.currentTimeMillis();
	        System.out.println("程序运行时间： "+(endTime-startTime)+"ms");
	         
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
