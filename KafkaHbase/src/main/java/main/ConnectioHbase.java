package main;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;

public class ConnectioHbase {

	TableName test = TableName.valueOf("test");
	 
	String fam = "dns";
	
	byte[] column1 = Bytes.toBytes("a");

	Configuration config = HBaseConfiguration.create();

	String path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();
	
	Connection connection;
	
	Admin admin;

	Table table;
	
	public ConnectioHbase() throws IOException, ServiceException {
		
		Conecta();
		
	}
	
	public void Conecta() throws IOException, ServiceException {

		config.addResource(new Path(path));

		connection = ConnectionFactory.createConnection(config);

		HBaseAdmin.checkHBaseAvailable(config);

		admin = connection.getAdmin();

		table = connection.getTable(test);
		
	}
	
	public void PutTable(String nome, int n_row) throws IOException, ServiceException {

		// Inserir linhas na tabela
		// Row1 => Family1:Qualifier1, Family1:Qualifier2

		String rw;

		rw = "row" + n_row + getDateTime();

		System.out.println("Linha PUT:" + rw);

		byte[] row_1 = Bytes.toBytes(rw);

		Put p = new Put(row_1);

		p.addImmutable(fam.getBytes(), column1, Bytes.toBytes(nome));

		table.put(p);

		// admin.disableTable(test);
		try {
			HColumnDescriptor desc1 = new HColumnDescriptor(row_1);
			admin.addColumn(test, desc1);
			System.out.println("Success.");
		} catch (Exception e) {
			System.out.println("Failed.");
			System.out.println(e.getMessage());
		} finally {
			// admin.enableTable(test);
		}
		// System.out.println("Done. ");
		// FImInserir linhas na tabela

	}
	
	private String getDateTime() {
		DateFormat dateFormat = new SimpleDateFormat("dd_MM_yyyyHHmmss");
		Date date = new Date();
		return dateFormat.format(date);
	}
	
	public void Close() throws IOException {
		connection.close();
	}
	
	
}
