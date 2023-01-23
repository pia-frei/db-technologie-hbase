package com.hbasedemo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;


@SpringBootApplication
public class HbaseApplication
{

	private static final byte[] COL_FAMILY_ADDRESS = toBytes("address");
	private static final byte[] COL_FAMILY_WORK = toBytes("work");
	private static final String USER_TABLE_NAME = "User";
	private static final byte[] ADDRESS_STREET = toBytes("street");
	private static final byte[] ADDRESS_NUMBER = toBytes("number");
	private static final byte[] ADDRESS_PLZ = toBytes("plz");

	public static void main(String[] args) throws IOException
	{
		SpringApplication.run(HbaseApplication.class, args);

		Configuration config = HBaseConfiguration.create();

		Configuration hBaseConfig =  HBaseConfiguration.create();
		hBaseConfig.set("hbase.zookeeper.quorum","localhost");
		hBaseConfig.set("hbase.zookeeper.property.clientPort", "2181");

		testConnection(config);

		Connection connection = ConnectionFactory.createConnection(config);
		Admin admin = connection.getAdmin();

		createTableUser(admin);
		Table userTable = connection.getTable(TableName.valueOf("User"));

		//create
		//Inhalt Tabelle ausgeben: scan 'User'
		Put createAddress = new Put(toBytes("row1"));
		createAddress.addColumn(COL_FAMILY_ADDRESS, ADDRESS_STREET, toBytes("Müllerstrasse"));
		createAddress.addColumn(COL_FAMILY_ADDRESS, ADDRESS_NUMBER, toBytes("17"));
		createAddress.addColumn(COL_FAMILY_ADDRESS, ADDRESS_PLZ, toBytes("30002"));
		userTable.put(createAddress);

		//get
		Get get = new Get(toBytes("row1"));
		get.addFamily(COL_FAMILY_ADDRESS);
		Result result = userTable.get(get);
		String street = Bytes.toString(result.getValue(COL_FAMILY_ADDRESS, ADDRESS_STREET));
		String number = Bytes.toString(result.getValue(COL_FAMILY_ADDRESS, ADDRESS_NUMBER));
		String plz = Bytes.toString(result.getValue(COL_FAMILY_ADDRESS, ADDRESS_PLZ));
		System.out.println(street + " " + number + " " + plz);

		//update
		Put updateStreetNumber = new Put(toBytes("row1"));
		updateStreetNumber.addColumn(COL_FAMILY_ADDRESS, ADDRESS_NUMBER, toBytes("25"));
		userTable.put(updateStreetNumber);

		//delete
		Delete delete = new Delete(toBytes("row1"));
		delete.addFamily(COL_FAMILY_ADDRESS);
		userTable.delete(delete);
	}

	private static void testConnection(Configuration config)
	{
		try
		{
			HBaseAdmin.available(config);
			System.out.println("Connection success!");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	private static void createTableUser(Admin admin) throws IOException
	{
		TableDescriptorBuilder tBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(USER_TABLE_NAME));
		ColumnFamilyDescriptor col_fam_address = ColumnFamilyDescriptorBuilder.newBuilder(COL_FAMILY_ADDRESS).build();
		ColumnFamilyDescriptor col_fam_work = ColumnFamilyDescriptorBuilder.newBuilder(COL_FAMILY_WORK).build();
		TableDescriptor tDesc = tBuilder.setColumnFamilies(Arrays.asList(col_fam_address,col_fam_work)).build();
		admin.createTable(tDesc);
	}

}
