package hbase.client.simpleAPI;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * 
 * @author Amine SELLAMI 
 * Cette classe présente des fonctions utilitaires pour hbase 1.0, il suffit de changer la config hbase
 * pour la bracher en dans le cluster 
 *
 */
public class HbaseUtils {

	private static final Logger log = Logger.getLogger(HbaseUtils.class);
	private static String zk_host;
	private static Configuration conf;

	static {
		// charger la config hbase
		zk_host = "";
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", zk_host);
	}

	private static final Connection connection = CustomizedHbaseConnection
			.createConnection(conf);

	public static void createTable(TableName tableName, String cf) {

		try (Admin admin = connection.getAdmin();) {
			// create table
			HTableDescriptor hTable = new HTableDescriptor(tableName);
			// create a column family for this table
			hTable.addFamily(new HColumnDescriptor(cf));
			if (!admin.tableExists(hTable.getTableName())) {
				System.out.print("Creating table. ");
				admin.createTable(hTable);
				System.out.println(" Done.");
			} else
				System.out.println("table " + hTable.getTableName()
						+ " already exists. ");
		} catch (IOException e) {
			log.error(e);
		}

	}

	public static void getSingleRecord(TableName tableName, String family,
			String rowKey) {

		try (Table table = connection.getTable(tableName);) {

			// instantiate a get with the rowkey to retrieve
			Get get = new Get(rowKey.getBytes());
			// we only want the data that belongs to this column family
			get.addFamily(Bytes.toBytes(family));

			// retrive the row with the specified get
			Result rs = table.get(get);
			printSingleRecord(rs);

			table.close();
		} catch (IOException e) {
			log.error(e);
		}
	}

	public void getAllRecords(TableName tableName) {

		try (Table table = connection.getTable(tableName);) {
			ResultScanner rs = table.getScanner(new Scan());

			for (Result r : rs) {
				printSingleRecord(r);
				rs.close();
			}
			table.close();
		} catch (IOException e) {
			log.error(e);
		}
	}

	public static void addOneRecord(TableName tableName, String rowKey,
			String family, String qualifier, String value) {

		try (Table table = connection.getTable(tableName);) {

			// we instantiate a Put providing the unique row key to the
			// constructor
			Put put = new Put(Bytes.toBytes(rowKey));

			// Add a value to the specified column qualifier
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),
					Bytes.toBytes(value));

			// execute put
			table.put(put);
			System.out.println("insert recored " + rowKey + " to table "
					+ tableName + " ok.");

			table.close();
		} catch (IOException e) {
			log.error(e);
		}
	}

	private static void printSingleRecord(Result rs) {
		for (Cell cell : rs.rawCells()) {
			System.out.print(new String(CellUtil.cloneRow(cell)) + " ");
			System.out.print(new String(CellUtil.cloneFamily(cell)) + ":");
			System.out.print(new String(CellUtil.cloneQualifier(cell)) + " ");
			System.out.print(new String(CellUtil.cloneValue(cell)) + " ");
			System.out.println(cell.getTimestamp());
		}

	}

}
